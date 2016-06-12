#!/usr/bin/env python
# -*- coding:utf8 -*-


from client.thrift.client import Client as ThriftClient
from store.cv_page_store import CVPageStore
from store.cv_raw_store import CVRawStore
from tools.savebin import BinReader
from tools import util
import traceback
import os


class DispatcherBase(object):
    def __init__(self, q):
        self.queue = q

    def load_data(self):
        pass

    def dispatcher(self, from_where='db'):
        self.real_dispatcher(from_where)

    def real_dispatcher(self, from_where):
        raise Exception("virtual function called")


class ETLDispatcherBase(DispatcherBase):
    def __init__(self, channel, q):
        DispatcherBase.__init__(self, q)
        self.channel = channel
        self.cv_page_store = CVPageStore(channel)
        self.process_item = {}

        # 二进制文件位置
        self.bin_file_location = os.environ['BIN_FILE_LOCATION']

        if self.bin_file_location == 'remote':
            self.thrift_client = ThriftClient(os.path.join(os.path.dirname(__file__), '../../../conf/thrift_conf.ini'))
            self.bin_read_client = self.thrift_client.bin_read_server_client

    def getPageContent(self, filename, from_where='local'):
        if from_where == 'local':
            parts = filename.split("::")
            if len(parts) == 3:
                binReader = BinReader(parts[1])
                _, content = binReader.readone_at(int(parts[2]))
                if len(content) == 0:
                    raise Exception("file name:{} , content error".format(filename))
                return content

            if len(parts) == 1:
                with open(filename) as f:
                    content = f.read()
                    if len(content) == 0:
                        raise Exception("file name:{} , content error".format(filename))
                    return content
        elif from_where == 'remote':
            #TODO
            # 从远程获取bin文件内容
            content = self.bin_read_client.getHtml(filename)
            if isinstance(content, unicode):
                content = content.encode('utf-8')
            return content

        else:
            raise Exception("unknown from_where")


class ETLDispatcher(ETLDispatcherBase):
    def __init__(self, channel, q):
        ETLDispatcherBase.__init__(self, channel, q)
        self.cv_raw_store = CVRawStore(self.channel, stage='raw')

    def fill_data_with_flag(self, indexUrl, realUrl, contentSign, updateTime, filePath, flag):

        self.process_item[indexUrl] = {
            'updateTime': updateTime,
            'contentSign': contentSign,
            'realUrl': realUrl,
            'filePath': filePath,
            'flag': flag,
        }

        if len(self.process_item)%10000 == 0:
            print "load items: %d" % len(self.process_item)

    def check_and_put(self, item):

        updateTime = item.get('updateTime')
        indexUrl = item.get('indexUrl')
        contentSign = item.get('contentSign')
        file_path = item.get('pageContentPath')
        realUrl = item.get('realUrl')

        self.fill_data_with_flag(indexUrl, realUrl, contentSign, updateTime, file_path, 0)

    def real_dispatcher(self, from_which):
        if from_which == 'db':
            self.load_data()
            self.dispatcher_from_db()
        # elif from_which == 'file':
        #     self.dispatcher_from_file()
        else:
            raise Exception("unknown from_which")

    def exist_in_raw(self, indexUrl):
        if self.cv_raw_store.get_one(indexUrl):
            return True

        return False

    def load_data(self):
        for item in self.cv_page_store.get_all():
            # if self.exist_in_raw(item['indexUrl']):
            #     continue
            self.check_and_put(item)

        print "============= totally load %d items ===============" % len(self.process_item)

    def dispatcher_from_db(self):

        i = 0
        total_cnt = len(self.process_item)
        for item in self.process_item:
            pagecontent = self.getPageContent(self.process_item[item].get("filePath"), self.bin_file_location)
            self.queue.put({'indexUrl': item, 'pagecontent': pagecontent, 'updateTime':self.process_item[item]['updateTime'], 'contentSign':
                   self.process_item[item]['contentSign']})

            i += 1
            if i % 10000 == 0:
                print "processed %f%%" % (float(i*100/total_cnt))

        self.queue.put(None)


class ETLDisFromFile(ETLDispatcher):
    def __init__(self, channel, q, file_name):
        ETLDispatcher.__init__(self, channel,q)
        self.file_name = file_name

    def real_dispatcher(self, from_which):
        self.dispatcher_from_file()

    def dispatcher_from_file(self):
        with open(self.file_name, 'rb') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                doc = self.cv_page_store.get_one(line)
                if not doc:
                    continue
                pagecontent = self.getPageContent(doc.get('pageContentPath'), self.bin_file_location)
                self.queue.put({'indexUrl': line, 'pagecontent': pagecontent, 'updateTime':doc['updateTime'], 'contentSign':
                   doc['contentSign']})

        self.queue.put(None)


class ETLDisFromRaw(ETLDispatcher):
    def load_data(self):
        pass

    def real_dispatcher(self, from_which):
        for item in self.cv_raw_store.get_all():
            self.queue.put(item)


class ETLDisFromRawAfterCheck(ETLDisFromRaw):
    '''
        检查解析表和量化表数据一致性，

        这个是粗略处理： 解析表中有的，如果量化表没有，重新解析量化
    '''

    def __init__(self, channel, q):
        ETLDisFromRaw.__init__(self, channel, q)
        self.cv_measure_store = CVRawStore(channel, 'measure')

    def real_dispatcher(self, from_which):
        index = 0
        for item in self.cv_raw_store.get_all():
            index += 1
            if not self.cv_measure_store.find_one({'cvId':item['cvId']}):
                self.queue.put(item)

            if index % 10000 == 0:
                print "copied %d items" % index


class IncEtlDispatcher(ETLDispatcher):

    # 爬虫二进制文件存放路径 模板                          渠道|年|渠道|年|月
    CRAWLER_BIN_FILE_TEMPLATE = '/data/crawler/_files3_/%s/%d/%s_%04d%02d.bin'
    BIN_FILE_LOCATE_HOST = 'hadoop2'

    PATH_INDEX_TMEPLATE="binf::%s::%d"

    def __init__(self, channel, q):
        ETLDispatcher.__init__(self, channel, q)

        self.pos_file_save_dir = os.path.join(os.path.dirname(__file__), self.channel)
        self.pos_file_save_path = '%s/%s_last_pos.dat' % (self.pos_file_save_dir, self.channel)
        # 今天爬虫存储的二进制文件
        self.last_copied_file, self.last_copied_pos = self.get_file_and_pos_from_saved_file()
        self.today_bin_file = self.get_today_bin_file_name()

        # 需要处理的文件
        self.need_copy_files_pos = self.get_need_copy_file_pos()

        print "need copy file :",  self.need_copy_files_pos

    def get_today_bin_file_name(self):
        today = util.get_date_with_day_duration(0)
        bin_file = IncEtlDispatcher.CRAWLER_BIN_FILE_TEMPLATE % (self.channel, today.year,
                                                                 self.channel, today.year, today.month)

        return bin_file

    def get_need_copy_file_pos(self):
        files_and_pos = []
        if self.last_copied_file:
            files_and_pos.append((self.last_copied_file, self.last_copied_pos))
        if self.today_bin_file == self.last_copied_file:
            return files_and_pos

        else:
            if self.today_bin_file:
                files_and_pos.append((self.today_bin_file, 0))

        return files_and_pos

    def get_file_and_pos_from_saved_file(self):
        if not os.path.exists(self.pos_file_save_path):
            return None, None

        filename = ""
        last_pos = 0
        ls = os.popen("tail -n 1 %s" % self.pos_file_save_path).read().strip()
        if ls:
            (filename, last_pos) = ls.split(":")

        return filename, int(last_pos)

    def real_dispatcher(self, from_which):
        cur_file = self.last_copied_file
        curpos = 0
        try:
            for filename, pos in self.need_copy_files_pos:

                cur_file = filename
                curpos = pos
                index_filepath = IncEtlDispatcher.PATH_INDEX_TMEPLATE % (filename, pos)

                print """========== starting to copy file: %s ============ """ % index_filepath

                i = 0

                while True:

                    i += 1
                    if i % 10000 == 0:
                        self._save_pos(cur_file, curpos)

                    c = self.bin_read_client.getBinContent(index_filepath)
                    if not c or not c.html:
                        break
                    curpos = c.lastPos

                    self.queue.put({
                        "indexUrl": "%s://%s" % (c.channel, c.cvOrJdId),
                        "pagecontent": c.html,

                    })

                    # offset 改变
                    index_filepath = IncEtlDispatcher.PATH_INDEX_TMEPLATE % (cur_file, curpos)

                print "======== finished copy file : %s =========" % index_filepath

        except Exception as e:
            traceback.print_exc()
        finally:
            self._save_pos(cur_file, curpos)

    def _save_pos(self, cur_file, pos):
        if not os.path.exists(self.pos_file_save_dir):
            os.mkdir(self.pos_file_save_dir)

        value = "%s:%d" % (cur_file, pos)
        with open(self.pos_file_save_path, 'a+b') as f:
            f.write(value + os.linesep)

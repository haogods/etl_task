# coding=utf-8

from etl_task import ETLTask


class Jd51ETLTask(ETLTask):
    def __init__(self, thread_cnt=20):
        ETLTask.__init__(self, "jd_51job", 500, thread_cnt)
        self.test_mode = False

    def run_job(self, item):
        cs = self.process_item[item].get('contentSign')
        try:
            if 1 == self.process_item[item].get('flag'):
                self.update_jd(item)
            elif 0 == self.process_item[item].get('flag'):
                self.parse_measure_jd(item)
            elif 2 == self.process_item[item].get('flag'):
                self._raw_store.set_raw_expired({"jdId": item})
                self._raw_store.set_measure_expired({"jdId": item})
                print "Set expired jdId: {}, Cs: {}".format(item, cs)
                return

            print "processed jdId: {}, Cs: {}".format(item, cs)
        except Exception as e:
            print "failed processed jdId: {}, Cs: {}".format(item, cs)
            raise e


if __name__ == '__main__':
    t = Jd51ETLTask(10)
    t.run()



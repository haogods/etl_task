#!/usr/bin/env python
# -*- coding:utf8 -*-


from flask import render_template, flash, redirect, request, send_from_directory
from forms import LoginForm
from controllers import cv_diff, crawler_statistics, encrypt_id, jd_html_show
from app import app

from flask import Flask, request, make_response, jsonify
from werkzeug.utils import secure_filename
import os
import threading



SHARED_PATH = os.path.join(os.path.dirname(__file__), app.config['SHARED_FOLDER'])

@app.route('/login', methods = ['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        flash('Login requested for OpenID="' + form.openid.data + '", remember_me=' + str(form.remember_me.data))
        return redirect('/index')

    return render_template('login.html',
        title='Sign In',
        form=form,
        providers = app.config['OPENID_PROVIDERS'])


@app.route('/')
@app.route('/index')
def index():
    user = {"nickname": 'woca'}
    return render_template('index.html', tp='CV', user=user)


@app.route('/uploads', methods=['GET', 'POST'])
def upload():
    if request.method == 'GET':
        return render_template('upload_file.html')
    elif request.method == 'POST':
        sample_file = request.files['sample_file']
        parse_file = request.files['parsed_file']
        if not sample_file or not parse_file:
            return render_template('upload_file.html', result='need upload files')

        parse_path = os.path.join(os.path.dirname(__file__), '%s/%s' % (app.config['SHARED_FOLDER'], parse_file.filename))
        sample_path = os.path.join(os.path.dirname(__file__), '%s/%s' % (app.config['SHARED_FOLDER'], sample_file.filename))

        sample_file.save(sample_path)
        parse_file.save(parse_path)

        cv_diff.save_parse_file(parse_path)
        cv_diff.save_sample_file(sample_path)

        s = threading.Thread(target=cv_diff.start_sample_diff, args=(sample_file.filename, ))
        s.start()
        s.join(100)
        # thread.start_new_thread(cv_diff.start_sample_diff, (sample_file.filename, ))

        return render_template('upload_file.html', result='upload succuss || ', ok=True)


@app.route('/shared')
def shared():
    fs = []
    for f in os.listdir(SHARED_PATH):
        if os.path.isfile(os.path.join(SHARED_PATH, f)):
            fs.append({'fname': os.path.split(f)[1]})

    return render_template('share.html', files=fs)


@app.route('/crawler', methods=['GET', ])
def crawler_count():
    jd_or_cv = request.args.get('jd_or_cv')
    channel = request.args.get('channel')

    if jd_or_cv not in ['cv', 'jd', 'co']:
        return "need jd or cv"
    if not channel:
        return "need channel"

    count = crawler_statistics.get_count(jd_or_cv, channel)

    r = {"channel": channel, "count": count}
    return make_response(jsonify(r))


@app.route('/measure', methods=['GET',])
def measure_count():
    channel = request.args.get('channel','')
    if not channel:
        return 'need channel'
    count = crawler_statistics.get_measure_count(channel)

    r = {"channel": channel, "count": count}
    return make_response(jsonify(r))


@app.route('/download/<fname>')
def download_file(fname):
    return send_from_directory(SHARED_PATH, fname, as_attachment=True)


@app.route('/cvid/encrypt')
def cvid_encrypt():
    cvid = request.args.get('cvid','')
    if not cvid:
        return make_response(jsonify({'flag':-1, 'msg':'need cv id'}))

    _id = encrypt_id.CEncryptID.encrypt(cvid)
    return make_response(jsonify({'flag':1, 'content': _id, 'msg': 'OK'}))


@app.route('/cvid/decrypt')
def cvid_decrypt():
    cvid = request.args.get('cvid','')
    if not cvid:
        return make_response(jsonify({'flag':-1, 'msg':'need cv id'}))

    _id = encrypt_id.CEncryptID.decrypt(cvid)
    return make_response(jsonify({'flag':1, 'content': _id, 'msg': 'OK'}))

@app.route('/showpage')
def show_page():
    jd_or_cv_id = request.args.get('id','')
    if 'jd' in jd_or_cv_id:
        return jd_html_show.GetHtmlPage.get_jd_html_page(jd_or_cv_id)
    if 'cv' in jd_or_cv_id:
        return jd_html_show.GetHtmlPage.get_cv_html_page(jd_or_cv_id)

    raise Exception('unkown jd_or_cv_id')



# def test():
#     while 1:
#         print 1
#         time.sleep(1)

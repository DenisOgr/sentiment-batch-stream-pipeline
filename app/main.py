from datetime import datetime

from flask import Flask, render_template, request, flash, redirect, url_for

import utils

app = Flask(__name__)
app.jinja_env.globals.update(get_random_style=utils.get_random_style)
import logging


@app.route('/', methods=['GET'])
def main():
    return render_template("index.html", jobs=utils.get_all_jobs())


@app.route('/run_job', methods=['POST'])
def run_job():
    string = request.form['word_for_search']
    try:
        utils.validate(string)
    except AssertionError as e:
        flash(str(e), 'errors')
        return redirect(url_for('main'))

    job_id = utils.get_job_from_string(string)
    job = utils.get_job(job_id)
    if job and not job.get_error_path():
        flash(f"Query: {string} has already been analysed. Job has been skipped.", "info")
        return redirect(url_for('job', string=string))

    job = {
        'query': string,
        'job_id': job_id,
        'created_at': str(datetime.now())
    }
    try:
        utils.store_job(job)
        return redirect(url_for('job', string=utils.get_string_from_job_id(job_id)))
    except Exception as e:
        flash('Some internal error. Please try in the future.', 'errors')
        logging.exception(e)
        return redirect(url_for('main'))


@app.route('/job/<string:string>', methods=['GET'])
def job(string: str):
    message = None
    if not string.startswith('test'):
        job_id = utils.get_job_from_string(string)
    else:
        job_id = string

    job = utils.get_job(job_id)
    if not job:
        message = f'There isn`t job for word: "{string}"'
    else:
        if job.get_error_path():
            message = f'Error for job for word: "{string}"'
    if message:
        flash(message, 'errors')
        return redirect(url_for('main'))
    try:
        job_results = utils.apply_window(utils.get_results(job))
    except FileNotFoundError:
        return render_template("job.html", meta=job.get_meta(), inprogress=True, data=[], labels=[])

    return render_template("job.html", meta=job.get_meta(), data=job_results.data,
                           labels=list(map(str, job_results.labels)))


if __name__ == '__main__':
    app.secret_key = 'my_secret_key'
    app.run(host="0.0.0.0", port=8080, debug=True)

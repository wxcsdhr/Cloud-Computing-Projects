from flask import Flask, request
import requests
import socket

app = Flask(__name__, static_folder='site', static_url_path='')

@app.route("/", methods=['GET', 'POST'])
def handle():
    loadBalancer = "http://project22loadbalancer-1271968039.us-east-1.elb.amazonaws.com:80/"
    if request.method == 'POST':
        lang = request.form['lang']
        prog = request.form['prog']
        # images for different langage
        print lang
        print prog
        print 'start posting'
        # inspired from [2]
        r = requests.post(loadBalancer, data = {'lang': lang, 'prog': prog})

        return r.text

    else:
        return app.send_static_file("index.html")

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=5000)

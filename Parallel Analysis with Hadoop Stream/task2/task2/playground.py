from flask import Flask, request
from docker import Client

# import socket

app = Flask(__name__, static_folder='site', static_url_path='')


@app.route("/", methods=['GET', 'POST'])
def handle():
    cli = Client(base_url='unix://var/run/docker.sock')
    if request.method == 'POST':
        lang = request.form['lang']
        prog = request.form['prog']
        # Implementation goes here.
        #
        # 1) Launch a container and run prog inside it
        # 2) Capture the output and return them as the response.
        #
        # Both stdout and stderr should be captured.

        ### BEGIN STUDENT CODE ###

        # images for different langage
        PYTHON_IMAGE = 'python_image'
        RUBY_IMAGE = 'ruby_image'
        LUA_IMAGE = 'lua_image'
        PERL_IMAGE = 'perl_image'
        currentImage = None;
        language = None;
        argument = None;

        # create a file to same prog
        if(lang == 'python'):
            currentImage = PYTHON_IMAGE
            language = "python"
            argument = '-c'
        elif(lang == 'ruby'):
            currentImage = RUBY_IMAGE
            language = 'ruby'
            argument = '-e'
        elif(lang == 'lua'):
            currentImage = LUA_IMAGE
            language = 'lua'
            argument = '-e'
        else:
            currentImage = PERL_IMAGE
            language = 'perl'
            argument = '-e'
        #creat container
        container = cli.create_container(image=currentImage, command=[language, argument, prog])
        cli.start(container=container.get('Id'))
        response = cli.logs(container, stdout=True, stream = True, stderr=False)
        result = ''
        for line in response:
            result+=line
        response = cli.logs(container, stdout=False, stream = True, stderr=True)
        for line in response:
            result+=line    
        ### END STUDENT CODE ###
        print result
        return result[:-1]

    else:
        return app.send_static_file("index.html")

if __name__ == '__main__':
    app.run(threaded=True, debug=True, host="0.0.0.0", port=5000)

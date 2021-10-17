from flask import Flask, jsonify, request, Response, render_template
from kafka import KafkaConsumer
import json

app = Flask(__name__)

@app.route("/")
def index():
    return(render_template('index.html'))

@app.route("/map")
def map():
    return(render_template('map.html'))
                            
@app.route('/topic/<topicname>')
def get_messages(topicname):
    client = KafkaConsumer(topicname)
    def events():
        for msg in client:
            data = json.loads(msg.value)
            if data.get('place') is not None:
                yield 'data:{0}\n\n'.format(msg.value.decode())
    return (Response(events(), mimetype="text/event-stream"))

if __name__ == "__main__":
    app.run(debug=True, port=5001)
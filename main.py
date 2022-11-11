# A very simple nosql database for storing and retrieving data, like Redis, but in Python via a tornado http server
# The http database simply stores key/value pairs, the key is always a string and the value is always binary data and can be anything.
# The content-type of the value is stored with the value, so the client can retrieve the value with the correct content-type.
# No advanced features like lists, sets, etc. are supported, all keys in the database can be retrieved with the GET command with a blank key.
# The database is not persistent, it is only in memory
# multiple clients can connect to the database at the same time and multiple databases can be created

# can be tested using curl, a browser, or a program like Postman (or online https://reqbin.com/)
# curl -X GET http://localhost:8888/db1/key1
# curl -X POST -d "value" http://localhost:8888/db1/key1
# curl -X PUT -d "value" http://localhost:8888/db1/key1
# curl -X DELETE http://localhost:8888/db1/key1

import tornado
from netifaces import interfaces, ifaddresses, AF_INET
import time
# tornado server
from tornado import gen, httpclient
from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer
from tornado.log import app_log
from tornado.options import define, options, parse_command_line
from tornado.web import Application, RequestHandler, URLSpec

# dictionary of dictionaries: database [key] = value
databases = {}

PORT = 8888
DEBUG = False
# Options
define("debug", default=DEBUG, help="Enable or disable debug", type=bool)
define("port", default=PORT, help="Run app on the given port", type=int)

# the database is a dictionary of key/value pairs
# the key is always a string
# the value is always bytes, stored as a class that contains the value,
# the size of the value, content-type, and the time it was last accessed, updated, or created
class Value:
    def __init__(self, value, content_type):
        self.value = value
        self.content_type = content_type
        self.size = len(value)
        self.last_modified = self.last_accessed = self.created = time.time()

    def update(self, value, content_type):
        self.value = value
        self.content_type = content_type
        self.size = len(value)
        self.last_modified = time.time()

    def access(self):
        self.last_accessed = time.time()

    def get(self):
        self.access()
        return self.value


def get_databasename_and_key(path):
    split_path = path.split('/')
    if len(split_path) < 2:
        return None, None
    databasename = split_path[1]
    if len(split_path) < 3:
        return databasename, None
    key = '/'.join(split_path[2:]) # join the rest of the path back together in case it has slashes
    return databasename, key

# http request handler, the first part of the path is the database name, the second part is the key
# if the key is not specified, then the key list is returned
class DatabaseHandler(RequestHandler):
    async def get(self):
        #print('GET', self.request.path)
        # get the database name, check if it exists, if not, create it
        databasename, key = get_databasename_and_key(self.request.path)
        if databasename not in databases:
            self.send_error(404)
            return
        database = databases[databasename]
        # if the key is not specified, return the list of keys
        if key is None:
            self.set_header('Content-type', 'text/plain')
            self.write('\r'.join(database.keys()).encode())
            return
        # if the key is specified, check if it exists, if not, return 404
        if key not in database:
            self.send_error(404)
            self.write(b'Key not found')
            return
        # if the key exists, return the value
        value = database[key]
        self.set_header('Content-type', value.content_type)
        self.set_header('Created', str(value.created))
        self.set_header('Last-Modified', str(value.last_modified))
        self.set_header('Last-Accessed', str(value.last_accessed))
        self.set_header('Size', str(value.size))
        self.write(value.get())

    # POST - create a new key/value pair if the key does not exist, otherwise return 409
    async def post(self):
        #print('POST', self.request.path)
        # get the database name, check if it exists, if not, create it
        databasename, key = get_databasename_and_key(self.request.path)
        if databasename not in databases:
            databases[databasename] = {}
        database = databases[databasename]
        # if the key is not specified, return 400
        if key is None:
            self.send_error(400)
            self.set_header('Content-type', 'text/plain')
            self.write(b'Key not specified')
            return
        # if the key is specified, check if it exists, if it does, return 409
        if key in database:
            self.send_error(409)
            self.set_header('Content-type', 'text/plain')
            self.write(b'Key already exists')
            return
        # if the key does not exist, create it
        content_type = self.request.headers.get('Content-type', 'text/plain')
        content_length = self.request.headers.get('Content-length', 0)
        database[key] = Value(self.request.body, content_type)
        # return 201
        self.set_status(201)
        self.set_header('Content-type', 'text/plain')
        self.write(b'Key created')
        return

    # PUT - create a new key/value pair if the key does not exist, otherwise update the value
    async def put(self):
        #print('PUT', self.request.path)
        # get the database name, check if it exists, if not, create it
        databasename, key = get_databasename_and_key(self.request.path)
        if databasename not in databases:
            databases[databasename] = {}
        database = databases[databasename]
        # if the key is not specified, return 400
        if key is None:
            self.send_response(400)
            self.set_header('Content-type', 'text/plain')
            self.end_headers()
            self.write(b'No key specified')
            return
        # if the key is specified, check if it exists, if it does, update the value
        content_type = self.request.headers.get('Content-type', 'text/plain')
        content_length = self.request.headers.get('Content-length', 0)
        value = self.request.body
        if key in database:
            database[key].update(value, content_type)
        else:
            database[key] = Value(value, content_type)
        # return 201
        self.set_status(201)
        self.set_header('Content-type', 'text/plain')
        self.write(b'Key created/updated: ' + key.encode())
        return

    # DELETE - delete the key/value pair if it exists, otherwise return 404
    async def delete(self):
        #print('DELETE', self.request.path)
        # get the database name, check if it exists, if not, create it
        databasename, key = get_databasename_and_key(self.request.path)
        if databasename not in databases:
            databases[databasename] = {}
        database = databases[databasename]
        # if the key is not specified, return 400
        if key is None:
            self.send_error(400)
            self.set_header('Content-type', 'text/plain')
            self.write(b'No key specified')
            return
        # if the key is specified, check if it exists, if it does, delete it
        if key in database:
            del database[key]
            # return 200
            self.set_status(200)
            self.set_header('Content-type', 'text/plain')
            self.write(b'Key deleted: ' + key.encode())
            return
        # if the key does not exist, return 404
        self.send_error(404)
        self.set_header('Content-type', 'text/plain')
        self.write(b'Key does not exist: ' + key.encode())
        return

    # HEAD returns the last updated time of the key
    async def head(self):
        #print('HEAD', self.request.path)
        # get the database name, check if it exists, if not, create it
        databasename, key = get_databasename_and_key(self.request.path)
        if databasename not in databases:
            # return 404
            self.send_error(404)
            self.set_header('Content-type', 'text/plain')
            self.write(b'Database does not exist: ' + databasename.encode())
            return
        
        database = databases[databasename]
        if key == '':
            # return 200
            self.set_status(200)
            self.set_header('Content-type', 'text/plain')
            self.write(b'Database exists: ' + databasename.encode())
            return
        # if the key is specified, return the last updated time
        if key in database:
            self.set_status(200)
            value = database[key]
            self.set_header('Content-type', value.content_type)
            self.set_header('Last-Modified', str(value.last_modified))
            self.set_header('Last-Accessed', str(value.last_accessed))
            self.set_header('Size', str(value.size))
            self.write(b'Key exists: ' + key.encode())
            return
        # if the key does not exist, return 404
        else:
            self.send_error(404)
            self.set_header('Content-type', 'text/plain')
            self.write(b'Key does not exist: ' + key.encode())
    def log_message(self, format, *args):
        return

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

# start the server
def start_server():
    print('Starting server on port %d' % options.port)
    # print all IP addresses on machine (also works on Windows, Mac, Linux)
    for interface in interfaces():
        addresses = ifaddresses(interface)
        if AF_INET in addresses:
            for address in addresses[AF_INET]:
                print(interface + ': http://' + address['addr'] + ':' + str(options.port))
            print()
    # start the server
    parse_command_line()
    app = Application([('/', MainHandler), ('/.*', DatabaseHandler)])
    app.settings['debug'] = options.debug
    app.listen(options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    start_server()




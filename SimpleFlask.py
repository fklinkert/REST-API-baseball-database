# Lahman.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request
import copy

import SimpleBO

#fields: string
#args: dictionary in pairs (our template)

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)

def parse_and_print_args():
    fields = None
    in_args = None
    if request.args is not None:
        in_args = dict(copy.copy(request.args))
        fields = copy.copy(in_args.get('fields', None))
        if fields:
            del(in_args['fields'])
    try:
        if request.data:
            body = json.loads(request.data)
        else:
            body = None
    except Exception as e:
        print("Got exception = ", e)
        body = None

    print("Request.args : ", json.dumps(in_args))
    return in_args, fields, body


@app.route('/api/<resource>', methods=['GET', 'POST'])
def get_resource(resource):
    
    in_args, fields, body = parse_and_print_args()
    print(in_args)
    if request.method == 'GET':
        if resource == 'roster':
            result = SimpleBO.roster(in_args)
        else:
            result = SimpleBO.find_by_template(resource, \
                                           in_args, fields)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}
    elif request.method == 'POST':
        try:
            SimpleBO.insert(resource,body)
            return "Method " + request.method + " on resource " + resource, \
            200, {"content-type": "text/plain; charset: utf-8"}
        except Exception as e:
            return "Method " + request.method + " on resource " + resource + \
            " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}
    else:
        return "Method " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}

@app.route('/api/<resource>/<primary_key>', methods=['GET', 'PUT', 'DELETE'])
def specific_resource(resource,primary_key):
    in_args, fields, body = parse_and_print_args()
    if request.method == 'GET':
        if resource == 'teammates':
            result = SimpleBO.teammates(primary_key)
        else:
            result = SimpleBO.find_by_primary_key(resource,primary_key,fields)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}
    elif request.method == 'DELETE':
        try:
            SimpleBO.delete(resource,primary_key)
            return "Method " + request.method + " on resource " + resource, \
            201, {"content-type": "text/plain; charset: utf-8"}
        except Exception as e:
            return "Method " + request.method + " on resource " + resource + \
            " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}
    elif request.method == 'PUT':
        try:
            SimpleBO.update(resource,primary_key,body)
            return "Method " + request.method + " on resource " + resource, \
            201, {"content-type": "text/plain; charset: utf-8"}
        except Exception as e:
            return "Method " + request.method + " on resource " + resource + \
            " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}
    else:
        return "Method " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}

@app.route('/api/<resource>/<primary_key>/<related_resource>', methods=['GET', 'POST'])
def dependent_resource(resource,primary_key,related_resource):
    in_args, fields, body = parse_and_print_args()
    if request.method == 'GET':
        if resource == "people" and related_resource == 'career_stats':
            result = SimpleBO.career_stats(primary_key)
        else:
            result = SimpleBO.get_by_foreign_key(resource,primary_key,\
                                            related_resource, in_args, fields)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}
    elif request.method == 'POST':
        try:
            SimpleBO.insert_by_foreign_key(resource,primary_key, \
                                           related_resource, body)
            return "Method " + request.method + " on resource " + resource, \
            201, {"content-type": "text/plain; charset: utf-8"}
        except Exception as e:
            return "Method " + request.method + " on resource " + resource + \
            " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}
    else:
        return "Method " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}


if __name__ == '__main__':
    app.run()
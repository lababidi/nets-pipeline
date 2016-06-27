import argparse

def status():
    print("Status request")

def load():
    print("Load articles ")

def initialize(es_index):
    print("Initialize %s" % es_index)


#
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--status', help='Display NETS status', action='store_true')
parser.add_argument('--load', help='load raw article files from the data directory', action='store_true')
parser.add_argument('--initialize',  default='NA', help='Initialize an index', choices=['articles','events'])


args = parser.parse_args()
if args.status :
    status()
elif args.load :
    load()
elif not args.initialize == 'NA':
    initialize( args.initialize)

#Command-line routine
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename",
                          help="write report to FILE", metavar="FILE")
parser.add_option("-t", "--format", dest="format",
                          help="Define the format of input RDF file", metavar="FORMAT")
parser.add_option("-s", action="store_true", dest="save",
                          help="(optional) Save to file?", metavar="SAVE")
parser.add_option("-l", action="store_true", dest="load",
                          help="(optional) Load from file?", metavar="LOAD")
(options, args) = parser.parse_args()

print options

import os
import uuid
import pickle
from lodstats import RDFStats
from lodstats.stats import lodstats as lodstats_set

class RDFFile(object):

    def __init__(self, filename, file_format):
        cwd = os.path.abspath(os.getcwd())
        system_prefix = 'file://'
        uri_path = ''.join([system_prefix, cwd, os.sep])
        self.uri_file = ''.join([uri_path, filename])
        self.id = "stats_" + str(filename)
        self.file_format = file_format
        self.rdfstats = None #runLODStats result
        self.stat_result = None

    def runLODStats(self, uri_file=None, file_format=None):
        if(not uri_file):
            uri_file = self.uri_file

        if(not file_format):
            file_format = self.file_format
        rdfstats = RDFStats(uri_file, format=file_format, stats=lodstats_set)
        rdfstats.parse(callback_fun=self.callback_parse)
        rdfstats.do_stats(callback_fun=self.callback_stats)

        self.rdfstats = rdfstats
        return rdfstats

    def callback_parse(self, rdfstats):
        stat_result = None
        stat_result.content_length = rdfstats.content_length
        stat_result.bytes_download = rdfstats.bytes_download
        stat_result.bytes = rdfstats.bytes
        print stat_result

    def callback_stats(self, rdfstats):
        if(rdfstats.no_of_statements > 0):
            if(rdfstats.no_of_statements % 10000 == 0):
                stat_result = None
                stat_result.triples_done = rdfstats.no_of_statements
                stat_result.warnings = rdfstats.warnings
                print stat_result

    def save_to_disk(self, stat_result=None, id=None):
        if(not stat_result):
            stat_result = self.stat_result

        if(not id):
            id = self.id

        file = open(str(id), "w")
        pickle.dump(stat_result, file)
        file.close()
        print "Dumped "+str(self.uri_file)+"to "+str(self.id)

    def load_from_disk(self, id=None):
        if(not id):
            id = self.id
        file = open(str(id), "r")
        stats_result = pickle.load(file)
        file.close()
        return stats_result

    def get_stat_result(self, rdfstats=None):
        stat_result = {}
        if(not rdfstats):
            rdfstats = self.rdfstats
        print rdfstats
        stat_result['triples'] = rdfstats.no_of_triples()
        stat_result['void'] = rdfstats.voidify('turtle')
        stat_result['warnings'] = rdfstats.warnings
        if(rdfstats.warnings > 0):
            stat_result['last_warning'] = unicode(rdfstats.last_warning.message, errors='replace')
        stat_result['has_errors'] = False
        stat_result['errors'] = None
        stat_result['stat_results'] = rdfstats.stats_results
        self.stat_result = stat_result
        return stat_result

rdffile = RDFFile(options.filename, options.format)
if(options.save):
    rdffile.runLODStats()
    rdffile.get_stat_result()
    rdffile.save_to_disk()

if(options.load):
    print rdffile.load_from_disk()

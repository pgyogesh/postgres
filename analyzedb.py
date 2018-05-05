from multiprocessing import Pool, Value
from pg import DB
import re
import logging
import optparse

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.DEBUG)

parser = optparse.OptionParser()
parser.add_option("-d", "--database", dest = "database", action = "store", help = "Specify target database to analyze")
parser.add_option("-n", "--schema", dest = "schema", action = "store", help = "Specify schema to analyze")
parser.add_option("-p", "--parallel-processes", dest = "parallel", action = "store",default = 1,help = "Specify number of parallel-processes")
parser.add_option("--host", dest = "host", action = "store", default = 'localhost', help = "Specify the target host")
parser.add_option("--user-tables", dest = "usertables", action = "store_true", help = "Specify if you want to analyze only user table")

options, args = parser.parse_args()

# Getting command line options

if options.database:
    vDatabase = options.database
else:
    logging.error("database not supplied... exiting...")
    sys.exit()

con = DB(dbname='postgres', host=options.host)
if vDatabase in con.get_databases():
    pass
else:
    logging.error("Database doesn't exists... exiting")
    sys.exit()
con.close()

vProcesses = int(options.parallel)
vHost = options.host
vSchema = options.schema

# Function to get list of table

def get_tables():
    db = DB(dbname = vDatabase, host = vHost)
    table_list = []
    if options.usertables:
        table_list = db.get_tables()
    else:
        table_list = db.get_tables('system')
    db.close()

    if vSchema:
        tables = []
        regex = "^" + vSchema + "\."
        for table in table_list:
            if re.match(regex, table, re.I):
                tables.append(table)
    else:
        tables = table_list
    return tables

counter = Value('i', 0)
total_tables = len(get_tables())

# Function to run analyze

def run_analyze(table):
    global counter
    db = DB(dbname = vDatabase, host = vHost)
    db.query('analyze %s' %table)
    with counter.get_lock():
        counter.value += 1
    if counter.value % 10 == 0 or counter.value == total_tables:
        logging.info(str(counter.value) + " tables completed out of " + str(total_tables) + " tables")
    db.close()

# For count
def init(args):
    ''' store the counter for later use '''
    global counter
    counter = args

# Forking new #n processes for run_analyze() Function

logging.info("Running analyze on " + str(total_tables) + " tables")
pool = Pool(initializer=init, initargs=(counter, ), processes=vProcesses)
pool.map(run_analyze, get_tables())

pool.close()  # worker processes will terminate when all work already assigned has completed.
pool.join()  # to wait for the worker processes to terminate.

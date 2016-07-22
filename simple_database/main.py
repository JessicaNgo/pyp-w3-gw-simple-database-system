import os
from config import BASE_DB_FILE_PATH

list_of_databases = os.listdir(BASE_DB_FILE_PATH) # ['library']

def create_database(db_name):
    
    if not os.path.exists(BASE_DB_FILE_PATH):
        os.makedirs(BASE_DB_FILE_PATH)

    if db_name in list_of_databases:
        msg = 'Database with name {} already exists.'.format(db_name)
        raise ValidationError(msg)
        
    list_of_databases.append(db_name)    
    return Database(db_name)

def connect_database(db_name):
    if db_name not in list_of_databases: 
        raise NotImplementedError()
    return db_name.Database()

class Database(object):
    
    def __init__(self, db_name):
        self.db_name = db_name
        self.path = BASE_DB_FILE_PATH + self.db_name + '.json'
        #self.path = '/tmp/'+db_name+'.json'
    
        #path = BASE_DB_FILE_PATH + db_name + suffix
    
    #@classmethod
    def create_table(self, table_name, columns=None):
        #import ipdb; ipdb.set_trace()
        with open(self.path, 'a') as db: #create file here
            #load json
            import ipdb; ipdb.set_trace()
            #print self.path # ==> '/tmp/simple_database/library.json'
            table = {} 
            #dump json?
            
                #write to file where we are containing columns
                #data structure lolloolololol :'(
            
        # db2.create_table('table1', columns=[
        #     {'name': 'id', 'type': 'int'},
        #     {'name': 'name', 'type': 'str'},
        #     {'name': 'birth_date', 'type': 'date'},
        #     {'name': 'nationality', 'type': 'str'},
        #     {'name': 'alive', 'type': 'bool'},
        # ])
            

    def show_tables():
        pass
import os
import json
from config import BASE_DB_FILE_PATH
from .exceptions import ValidationError
#need the following fcns:
#object = create_database()
#object = connect_database(db name hhere)
#object.show_tables() #return list of table names
#object.create_table('tablename', columns = [..])
#object.tablename.count()
#object.tablename.insert() #insert a row
#object.tablename.describe() #return column and their configs
#iterable = self.db.authors.query(nationality='query_Here')
#^should be able to do for item in iterable: print somethinghere
#```python
# >>> gen = self.db.authors.all()

# schema = {
#             table_name:{
#                 'columns':[
#                     #add columns in here eg. {'col_name': 'id', 'type': 'int'}
                    
#                     ]
#                 'data':[
#                     #add data in here eg. 
#                     #(id, name, birth_date, nationality, alive)
#                     #(id2, name2, birth_date2, nationality2, dead)
#                     #(id3, name3, birth_date3, nationality3, alive)
#                 ]
#             }
#         }     

print BASE_DB_FILE_PATH

try:
    list_of_databases = os.listdir(BASE_DB_FILE_PATH) # ['library']
except:
    if not os.path.exists(BASE_DB_FILE_PATH):
        os.makedirs(BASE_DB_FILE_PATH)
        list_of_databases = []
def create_database(db_name):
    
    if not os.path.exists(BASE_DB_FILE_PATH):
        os.makedirs(BASE_DB_FILE_PATH)
        
    if db_name in list_of_databases:
        msg = "Database with name '{}' already exists.".format(db_name)
        raise ValidationError(msg)
        
    list_of_databases.append(db_name)
    returned_object = Database(db_name)
    
    return returned_object


def connect_database(db_name):
    if db_name not in list_of_databases: 
        raise NotImplementedError()
    print db_name, list_of_databases
    CONNECT_DB_FILE_PATH = BASE_DB_FILE_PATH + db_name + ".json"
    with open(CONNECT_DB_FILE_PATH, 'r') as read_json_file:
        try: 
            saved_db = json.load(read_json_file)
        except: #empty json file?
            saved_db = {}
            might_as_create_new_db = Database(db_name)
            return might_as_create_new_db
        #if not empty : parse away!
        
        reinstance_db = Database(db_name)
        for table_name in saved_db: #create table object for each table in database
            columns = saved_db[table_name]['columns']
            reinstance_db.create_table(table_name, columns)

            data_list = saved_db[table_name]['data']
            import pprint;
            pprint.pprint(data_list)
            
            for data in data_list: #insert data row in object for each data row in json file
                getattr(reinstance_db, table_name).data = [] 
                getattr(reinstance_db, table_name).data.append(data)
        return reinstance_db
# ---> stepping away for a bit / afk <--------- pls someone send sleep to me
#ok i think thats done lol
# schema = {
#             table_name:{
#                 'columns':[
#                     #add columns in here eg. {'col_name': 'id', 'type': 'int'}
                    
#                     ]
#                 'data':[
#                     #add data in here eg. 
#                     #(id, name, birth_date, nationality, alive)
#                     #(id2, name2, birth_date2, nationality2, dead)
#                     #(id3, name3, birth_date3, nationality3, alive)
#                 ]
#             }
#         }     

    # file = json.load()? 
    # then for in file: and create table? :thumb up:  
    
    #prety much, im just thinking about what we have to iterate through, my processing power is like 0.000001x my normal speed
    #the database class hhas table objects as its attributes, which can be used to build json files
class Database(object): #does show_tables and create_table && create/connect database
    
    def __init__(self, db_name):
        self.db_name = db_name
        self.path = BASE_DB_FILE_PATH + self.db_name + '.json'
        # empty_dict = {'make me':2}
        self.list_of_table_names = []
        with open(self.path, 'w') as self.database_file:
            pass
            # json.dump(empty_dict, self.database_file)
            # self.database_file.write('hello')

    def create_table(   self, table_name, columns=None):
        """create attribute of class that is an object Table() class"""
        t_object = Table(table_name, columns, self.path) #instance table object
        setattr(self, table_name, t_object) #saves the table object as table_name
        self.list_of_table_names.append(table_name) #append table object
        t_object.db_name = self.db_name
        
                
        return t_object #return table object
    
    def show_tables(self):
        return self.list_of_table_names

class Table(object): #needs to take care of count() insert() describe() query() and all()
    def __init__(self, table_name, columns, path):
        self.table_name = table_name
        self.columns_list = columns
        self.data = []
        self.path = path
        
        with open(self.path, 'r+') as self.database_file:
                
            try:
                current_db = json.load(self.database_file)
            except:
                current_db = {}
            
            current_db[self.table_name] = {} #add table dict to data base under key table_name
            current_db[self.table_name]['columns']= self.columns_list
            current_db[self.table_name]['data'] = self.data
            json.dump(current_db, self.database_file, sort_keys=True, indent=4)
 
        
    def insert(self, *args):
        if len(args) != len(self.columns_list): #check parameters
            raise ValidationError('Invalid amount of field')
        
        formatted_args = []
            
        for index, arg in enumerate(args): #check if argument types valid
            arg_type = self.columns_list[index]['type']
            
            if type(arg).__name__ != arg_type:
                errormsg = ('Invalid type of field "{}": Given "{}", expected "{}"').format(self.columns_list[index]['name'], type(arg).__name__, arg_type)
                raise ValidationError(errormsg)
            if type(arg).__name__ == 'date':
                serialdate = arg.isoformat()
                formatted_args.append(serialdate)
            else:
                formatted_args.append(arg)
        
        arg_tuple =tuple(formatted_args)
        # print formatted_args,args
        
        #passed check, have to format 
        self.data.append(arg_tuple)
        
        with open(self.path, 'r+') as self.database_file:
            self.current_db = json.load(self.database_file)
            self.current_db[self.table_name]['data'] = self.data #update data
        with open(self.path, 'w') as self.database_file:
            json.dump(self.current_db, self.database_file)
    
    def describe(self):
        return self.columns_list
        
    def count(self):
        self.counter = len(self.data)
        return self.counter
    
    def query(self,**kwargs): #query(nationality = "ARG")
        #first need to find what index the column of key word is in
        result_list = []
        
        for query_key, query_value in kwargs.items(): #for each key word parameter
            for index, column in self.columns_list: #for each index, column in column list
                if column['colname'] == query_key: #if the col_name == query key, we are looking in the right column 
                    for data_tuple in self.data: #start looking at this index in data
                        if query_value in data_tuple[index]: #if the query value is in the tuple index
                            result_list.append(data_tuple) #add to result list
                    
                    
        return result_list
            
    def all(self):
        for value in self.data:
            yield value
        
        
        
    #     #import ipdb; ipdb.set_trace()
    #     with open(self.path, 'r+') as self.database_file: #create file here
    #         self.saved_dict = json.load(self.database_file)
            
    #         self.saved_dict[table_name] = {}
    #         setattr(self, table_name, {}) #set table_name as an attribute of the database class
            
    #         self.columns = columns #save the columns away heh
            
    #         self.table_name['columns'] = [] #to save as attribute of class
    #         self.table_name['data'] = [] #to save table as attribtue of class
            
    #         self.saved_dict[table_name]['columns'] = [] #to use for json file
    #         self.saved_dict[table_name]['data'] = [] #to use for json file
            
           
    #                 self.table_name['columns'].append(column)
    #                 self.saved_dict[table_name]['columns'].append(column) #access key columns list value, and append
            
    #         self.database_file.write(json.dumps(self.saved_dict))
    #             #alternatively can use as json.dump(self.saved_dict, self.database_file)
            
    #         # db2.create_table('table1', columns=[
    #         #     {'name': 'id', 'type': 'int'},
    #         #     {'name': 'name', 'type': 'str'},
    #         #     {'name': 'birth_date', 'type': 'date'},
    #         #     {'name': 'nationality', 'type': 'str'},
    #         #     {'name': 'alive', 'type': 'bool'},
    #         # ])
          
            
    
    # def insert(self, *args):
        
    #     with open(self.path, 'r+') as self.database_file: #open my file yee
    #         self.saved_dict = json.load(self.database_file)
    #         for index, arg in args:
    #             if isinstance(arg, self.saved_dict[self.table_name])
    #         self.saved_dict[table_name]['data'].append({})
            
    #             #write to file where we are containing columns
    #             #data structure lolloolololol :'(
    

    # def show_tables():
    #     pass
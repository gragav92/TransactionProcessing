class Database:
    def __init__(self, size):
        #Constructor for the Database Class
        self.db = []
        self.db_size = size
        self.s_lock = [False]*self.db_size
        self.x_lock = [False]*self.db_size
        for i in range(db_size):
            self.db.append(0.0)

    def __init__(self, size, items):
        #Constructor if item values are provided
        self.db = []
        self.db_size = size
        self.s_lock = [False]*self.db_size
        self.x_lock = [False]*self.db_size
        self.waiting_transactions = []
        self.transaction = []
        for i in items:
            self.db.append(i)

    def Read(self, index):
        #Function to read the database item at the given index
        return self.db[index]
    
    def Write(self, index, value):
        #Function to update the database item at the index with the value
        self.db[index] = value

    def Print(self):
        #Function to print the database in the required format
        for i in range(self.db_size):
            print(self.db[i])
            if (i+1)%5==0:
                print("\n")
        
#Class to hold a transaction information:        
class Transaction:
    def __init__(self, transaction, variables, commands):
        self.transaction_id = transaction
        self.num_commands = commands
        self.num_variables = variables
        self.local_mem = [0.0]*self.num_variables
        self.db_write_values = []
        self.instruction = []
        self.x_lock_values = []
        self.s_lock_values = []
        self.next_ins = 0
        self.commit = False

    def read_transaction(self, index):
        #Function to read from the transaction local memory
        return self.local_mem[index]
    
    def update_transaction(self, index, value):
        #Function to update the transaction local memory
        self.local_mem[index] = value
    
    def add_value(self, index, f):
        #Function to add the value of f to the variable at index
        self.local_mem[index] += f
    
    def multiply_value(self, index, f):
        #Function to multiply the value of f to the variable at index
        self.local_mem[index] *= f
    
    def copy_value(self, index_1, index_2):
        #Function to copy values from index_! to index_2
        self.local_mem[index_2] = self.local_mem[index_1]

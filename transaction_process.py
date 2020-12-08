from transaction_models import Database, Transaction
import random
import sys

# Function to check if to grant requested lock 'S' for shared 'X' for exlusive
def request_lock(db, type, lock_var):
    if type == 'S':
        if not db.s_lock[lock_var]:
            db.s_lock[lock_var]=True
            return True
        else:
            if db.x_lock[lock_var]:
                return False
            return True
    elif type== 'X':
        if not db.x_lock[lock_var]:
            if not db.s_lock[lock_var]:
                db.s_lock[lock_var]=True
            db.x_lock[lock_var]=True
            return True
        else:
            return False

# Function to check if the current transaction is complete
def check_transaction_complete(t, db):
    if db.transaction[t].next_ins == len(db.transaction[t].instruction):
        locks_released = 0
        # Set the transaction as committed if instruction set is finished
        db.transaction[t].commit = True
        # If the transaction is still in the waiting list, remove it
        if db.transaction[t] in db.waiting_transactions:
            db.waiting_transactions.remove(db.transaction[t])
        # Release the locks on the database values that the transaction holds
        for db_value in db.transaction[t].s_lock_values:
            db.s_lock[db_value] = False
            locks_released += 1
        for db_value in db.transaction[t].x_lock_values:
            db.x_lock[db_value] = False
            locks_released += 1
        # Print the completed output with the no. of locks released
        print("Transaction ",db.transaction[t].transaction_id," commits: locks released : ", locks_released)
        # Reset the lists of values held by slock or xlock by the transaction
        db.transaction[t].s_lock_values=[]
        db.transaction[t].x_lock_values=[]
        return True
    return False

# Function to check if all transactions are finished
def check_transactions_finished(db):
    for i in range(len(db.transaction)):
        if not db.transaction[i].commit:
            return False
    return True

# Function to check deadlock
def check_deadlock(db, t):
    deadlock = False
    # Loop through the waiting transactions
    # If transaction i requests lock on item locked by transaction j and
    # transaction j requests lock on item locked by transaction i then deadlock occurs
    # else no deadlock
    for i in db.waiting_transactions:
        i_request = i.instruction[i.next_ins].split(" ")
        if i is not db.transaction[t]:
            new_request = db.transaction[t].instruction[db.transaction[t].next_ins].split(" ")
            if int(i_request[1]) in db.transaction[t].x_lock_values :
                if int(new_request[1]) in i.x_lock_values:
                    print("Deadlock occurs: transactions that are not finished: ",db.waiting_transactions)
                    deadlock = True
    return deadlock

def rollback_transaction(db,t):
# Rollback the transaction
    while db.transaction[t].next_ins > 0:
        ins = db.transaction[t].instruction[db.transaction[t].next_ins].split(" ")
        if ins[0]=='W':
            db.Write(int(ins[1]), db.transaction[t].db_write_values[0])
        db.transaction[t].next_ins -= 1
    db.transaction[t].local_mem = [0.0]*db.transaction[t].num_variables
    db.transaction[t].s_lock_values = []
    db.transaction[t].x_lock_values = []
    db.transaction[t].db_write_values = []

# Function to implement wait_die_prevention of deadlock
def wait_die_prevent(db, t):
    for i in db.waiting_transactions:
        i_request = i.instruction[i.next_ins].split(" ")
        if i is not db.transaction[t]:
            new_request = db.transaction[t].instruction[db.transaction[t].next_ins].split(" ")
            if int(i_request[1]) in db.transaction[t].x_lock_values :
                if int(new_request[1]) in i.x_lock_values:
                    # if new transaction causes deadlock, rollback the new transaction 
                    # old transaction ==> lower transaction id
                    # younger transaction ==> higher transaction id
                    if db.transaction[t].transaction_id > i.transaction_id:      
                        print("Transaction ",db.transaction[t].transaction_id,", step",db.transaction[t].next_ins,": Aborted")
                        rollback_transaction(db, t)
                    else:
                    # if old transaction, then wait
                        db.waiting_transactions.append(db.transaction[t])
    


# FUnction that processes the transactions in random order
def process_transactions(db, wait_die):
    while not check_transactions_finished(db):
        # Randomly choose a transaction
        t = random.randrange(0, len(db.transaction))
        if not check_deadlock(db,t):
            # Retrive the next instruction for the transaction
            ins = db.transaction[t].instruction[db.transaction[t].next_ins].split(" ")
            
            #For below two conditions we need database access
            if ins[0]=='R':        
                if request_lock(db, 'S', int(ins[1])):
                    # S Lock has been granted, proceed with the operation
                    print("Transaction ",db.transaction[t].transaction_id,", step",db.transaction[t].next_ins,": request S-lock on ",ins[1],": Granted")
                    if db.transaction[t].transaction_id in db.waiting_transactions:
                        db.waiting_transactions.remove(db.transaction[t])
                    db.transaction[t].update_transaction(int(ins[2]), db.Read(int(ins[1])))
                    # Add the index of database item for which s lock has been granted to the transaction s_lock_values list
                    db.transaction[t].s_lock_values.append(int(ins[1]))    
                    db.transaction[t].next_ins += 1
                else:
                    print("Transaction ",db.transaction[t].transaction_id,", step",db.transaction[t].next_ins,": request S-lock on ",ins[1],": Not Granted")
                    db.waiting_transactions.append(db.transaction[t])


            elif ins[0]=='W':
                if request_lock(db, 'X', int(ins[1])):
                    # X lock has been granted, proceed with the operation
                    print("Transaction ",db.transaction[t].transaction_id,", step",db.transaction[t].next_ins,": request X-lock on ",ins[1],": Granted")
                    if db.transaction[t].transaction_id in db.waiting_transactions:
                        db.waiting_transactions.remove(db.transaction[t])
                    db.transaction[t].db_write_values.append(db.Read(int(ins[1])))
                    db.Write(int(ins[1]), db.transaction[t].read_transaction(int(ins[2])))
                    # Add the index of database item for which s lock has been granted to the transaction s_lock_values list
                    db.transaction[t].x_lock_values.append(int(ins[1]))
                    db.transaction[t].next_ins += 1
                else:
                    print("Transaction ",db.transaction[t].transaction_id,", step",db.transaction[t].next_ins,": request X-lock on ",ins[1],": Not Granted")
                    db.waiting_transactions.append(db.transaction[t])

            #For below three condtions its just local memory access
            elif ins[0]=='A':
                db.transaction[t].add_value(int(ins[1]), float(ins[2]))
                db.transaction[t].next_ins += 1
            elif ins[0]=='M':
                db.transaction[t].multiply_value(int(ins[1]), float(ins[2]))
                db.transaction[t].next_ins += 1

            elif ins[0]=='C':
                db.transaction[t].copy_value(int(ins[1]), int(ins[2]))
                db.transaction[t].next_ins += 1
            # Check if the current transaction is complete
            if check_transaction_complete(t, db):
                del db.transaction[t]
                continue
        else:
            if wait_die == True:
                wait_die_prevent(db, t)
                continue
            else:
                exit

def main():        
    #Read the input text file from the commandline
    wait_die_check = input("Do you want to enable Wait-Die deadlock prevention ? (y/n) :")
    wait_die = False
    if wait_die_check=='y' or wait_die_check=='Y':
        wait_die = True
    db_input_file = sys.argv[1]
    with open(db_input_file,"r") as db_input:
        n_items = int((db_input.readline()).rstrip())
        db_values_str = (db_input.readline()).rstrip()
        db_values_str = db_values_str.split(' ')
        items = [float(x) for x in db_values_str]
        n_transactions = int((db_input.readline()).rstrip())
        db = Database(n_items, items)
        for i in range(n_transactions):
            transaction_file = db_input.readline().rstrip()
            with open(transaction_file,"r") as t_input:
                transaction_details_str = (t_input.readline().rstrip()).split(' ')
                transaction_detail = [int(x) for x in transaction_details_str]
                db.transaction.append(Transaction(transaction_detail[0], transaction_detail[1], transaction_detail[2]))
                for i in range(transaction_detail[2]):
                    db.transaction[-1].instruction.append(t_input.readline().rstrip())

    process_transactions(db, wait_die)
    db.Print()




if __name__=="__main__": 
    main() 
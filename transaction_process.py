from transaction_models import Database, Transaction
import sys

#Read the input text file from the commandline
transaction = []
db_input_file = sys.argv[1]
with open(db_input_file,"r") as db_input:
    n_items = int((db_input.readline()).rstrip())
    db_values_str = (db_input.readline()).rstrip()
    db_values_str = db_values_str.split(' ')
    items = [float(x) for x in db_values_str]
    n_transactions = int((db_input.readline()).rstrip())
    database = Database(n_items, items)
    for i in range(n_transactions):
        transaction_file = db_input.readline().rstrip()
        with open(transaction_file,"r") as t_input:
            transaction_details_str = (t_input.readline().rstrip()).split(' ')
            transaction_detail = [int(x) for x in transaction_details_str]
            transaction.append(Transaction(transaction_detail[0], transaction_detail[1], transaction_detail[2]))
            for i in range(transaction_detail[2]):
                transaction[-1].instruction.append(t_input.readline().rstrip())
for t in transaction:
    print (t.instruction )

database.Print()
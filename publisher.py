import pika,random, time,threading
import tkinter as tk
import sqlite3
import random
con = sqlite3.connect("Transactions.db")

cur = con.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS Accounts (
    account_number INTEGER PRIMARY KEY,
    balance DECIMAL(12, 2) NOT NULL DEFAULT 1000
);""")

con.close()

def publish(sender,reciever,amount):

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    channel = connection.channel()

    channel.exchange_declare(exchange="card", exchange_type="topic")

    route_id = f"card.{reciever}"
    
    if reciever < 5000:
        message = str(sender) + "." + str(reciever) + "." + str(amount)
    

        channel.basic_publish(
            exchange="card",
            routing_key=route_id,
            body=message,properties=pika.BasicProperties(
                        delivery_mode = pika.DeliveryMode.Persistent))
        print(f" [x] Sent {reciever}:{amount}")

        time.sleep(0.25)
    elif reciever >= 5000:
        message = str(sender) + "." + str(reciever) + "." + str(amount)
    
        

        channel.basic_publish(
            exchange="card",
            routing_key=route_id,
            body= message,properties=pika.BasicProperties(
                        delivery_mode = pika.DeliveryMode.Persistent))
        print(f" [x] Sent {reciever}:{amount}")
    
        time.sleep(0.25)

    connection.close()


def threads(sender,reciever,amount):
    thread = threading.Thread(target=publish, args=(sender, reciever, amount), daemon=True)
    thread.start()







def rand():
    x = random.randint(1000,10000)
    return x




def acc_number():

    con = sqlite3.connect("Transactions.db")
    print("acc_number connection open")
    cur = con.cursor()
    

    while True:
        
        acc_number = rand()

        cur.execute("SELECT * FROM Accounts where account_number = ?", (acc_number,))

        exists = cur.fetchone() is not None

        if exists:
            continue
        else:
            con.execute("BEGIN TRANSACTION")
            cur.execute("INSERT INTO Accounts (account_number, balance) VALUES (?,?)", (acc_number, 1000.00))
            con.commit()
            con.close()
            print("acc_number connection closed")
            return acc_number

def balance(acc_num):
    
    con = sqlite3.connect("Transactions.db")
    cur = con.cursor()
   
    cur.execute("SELECT balance FROM Accounts WHERE account_number = ?", (acc_num,))
    balance = cur.fetchone()
    con.close()
    return balance[0]


def balance_thread(acc_num):
    thread = threading.Thread(target=balance,args = (acc_num), daemon=True)
    thread.start()

def send(sender_acc, reciever_acc, amount_entry):
    
    sender = sender_acc
    reciever = int(reciever_acc.get())
    amount = int(amount_entry.get()) 
    con = sqlite3.connect("Transactions.db")
    print("Send connection opened")
    cur = con.cursor()
    con.execute('BEGIN TRANSACTION')


    cur.execute("SELECT balance FROM Accounts WHERE account_number = ?", (sender,))
    balance = cur.fetchone()
    balance = balance[0]


    if amount > balance:
        show_error("NOT ENOUGH MONEY")
        con.commit()
        con.close()
        print("Send connection closed")
        return
    
    cur.execute("SELECT account_number FROM Accounts WHERE account_number = ?", (reciever,))
    temp = cur.fetchone()
    if not temp:
        show_error("Receiver account doesn't exist")
        con.commit()
        con.close()
        print("Send connection closed")
        return
    con.close()
    threads(sender,reciever,amount)
     

    
    

        
def show_error(message):
    win = tk.Toplevel()
    win.title("ERROR")
    win.geometry("200x100")
    tk.Label(win, text=message).pack(pady=20)
    

def give_away(acc):

    con = sqlite3.connect("Transactions.db")
    cur = con.cursor()

    cur.execute("SELECT account_number from Accounts")

    accounts = cur.fetchall()
    
    num_array = [a[0] for a in accounts]

    count = len(num_array)

    cur.execute("SELECT balance FROM Accounts WHERE account_number = ?", (acc,))

    tmp = cur.fetchall()
    con.close()
    balance = tmp[0][0]

    if balance % 2 == 0:
        for i in num_array:
            publish(acc,i,2)
    else:
        for i in num_array:
            publish(acc,i,1)















class UI():
    
    def __init__(self):

        self.root = tk.Tk()
        self.root.title("MainWindow")
        self.root.geometry("500x500")
        account = acc_number()
        self.label = tk.Label(self.root, text=f'Bank account number ****{account}')
        self.label.pack(anchor='w',pady=10, padx=20)
        money = balance(account)
        self.label2 = tk.Label(self.root, text=f'Balance: {money}')
        self.label2.pack(anchor='w', padx=20)

        self.input_label = tk.Label(self.root,text="Account Number")
        self.input_label.pack(anchor='w', padx=20,pady=10)

        self.input_number = tk.Entry(self.root, width=30)
        self.input_number.pack(anchor='w', padx=20)

        self.input_label2 = tk.Label(self.root, text="Amount:")
        self.input_label2.pack(anchor='w', padx=20, pady=10)

        self.input_number2 = tk.Entry(self.root, width=30)
        self.input_number2.pack(anchor='w', padx=20)

        self.publish = tk.Button(self.root,text="Send", command=lambda:send(account, self.input_number, self.input_number2))
        self.publish.pack(pady=10)

        self.refresh = tk.Button(self.root,text="Refresh", command=lambda:self.update_balance(account))
        self.refresh.pack(pady=30)

        self.random = tk.Button(self.root, text = "Give money to each account", command=lambda:give_away(account))
        self.random.pack(pady=40)

    def update_balance(self,account):
        money = balance(account)
        self.label2.config(text=f'Balance: {money}')
    

    def run(self):
        self.root.mainloop()


run = UI()

run.run()
    





    



    


    


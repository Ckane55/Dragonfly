import pika,random, time,threading
import tkinter as tk
import sqlite3
import random
import psycopg2
con = psycopg2.connect(
    dbname="defaultdb",  # CockroachDB default database
    user="root",         # default user in --insecure mode
    password="",         # no password for --insecure
    host="localhost",
    port=26257           # Node 1 SQL port
)
con.autocommit = True

cur = con.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS Accounts (
    account_number INTEGER PRIMARY KEY,
    balance DECIMAL(12, 2) NOT NULL DEFAULT 1000
);""")

con.commit()


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

   
    print("acc_number connection open")
    cur = con.cursor()
    

    while True:
        
        acc_number = rand()

        cur.execute("SELECT * FROM Accounts where account_number = %s", (acc_number,))

        exists = cur.fetchone() is not None

        if exists:
            continue
        else:
            
            cur.execute("INSERT INTO Accounts (account_number, balance) VALUES (%s,%s)", (acc_number, 1000.00))
            con.commit()
            
            print("acc_number connection closed")
            return acc_number

def balance(acc_num):
    
    
    cur = con.cursor()
   
    cur.execute("SELECT balance FROM Accounts WHERE account_number = %s", (acc_num,))
    balance = cur.fetchone()
    
    return balance[0]


def balance_thread(acc_num):
    thread = threading.Thread(target=balance,args = (acc_num), daemon=True)
    thread.start()

def send(sender_acc, reciever_acc, amount_entry):
    
    sender = sender_acc
    reciever = int(reciever_acc.get())
    amount = int(amount_entry.get()) 
    
    print("Send connection opened")
    cur = con.cursor()
    


    cur.execute("SELECT balance FROM Accounts WHERE account_number = %s", (sender,))
    balance = cur.fetchone()
    balance = balance[0]


    if amount > balance:
        show_error("NOT ENOUGH MONEY")
        con.commit()
        
        print("Send connection closed")
        return
    
    cur.execute("SELECT account_number FROM Accounts WHERE account_number = %s", (reciever,))
    temp = cur.fetchone()
    if not temp:
        show_error("Receiver account doesn't exist")
        con.commit()
        
        print("Send connection closed")
        return
    con.commit()
    threads(sender,reciever,amount)
     

    
    

        
def show_error(message):
    win = tk.Toplevel()
    win.title("ERROR")
    win.geometry("200x100")
    tk.Label(win, text=message).pack(pady=20)
    

def give_away(acc):

   for x in range(3):
       
        cur = con.cursor()

        cur.execute("SELECT account_number from Accounts")

        accounts = cur.fetchall()
        
        num_array = [a[0] for a in accounts]

        count = len(num_array)

        cur.execute("SELECT balance FROM Accounts WHERE account_number = %s", (acc,))

        tmp = cur.fetchall()
        con.commit()
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
        self.root.configure(bg="gray")

        account = acc_number()
        self.label = tk.Label(self.root, text=f'Bank account number ****{account}', bg="Gray", foreground="White",font=("Ariel",20))
        #self.label.pack(anchor='w',pady=10, padx=20)
        money = balance(account)
        self.label2 = tk.Label(self.root, text=f'Balance: {money}', bg= "Gray", foreground="White",font=("Ariel",16,"bold"))
        #self.label2.pack(anchor='w', padx=20)
        self.label.grid(row=0, column=0,sticky="w", padx=5, pady=10)
        self.label2.grid(row=1, column=0, sticky="w",padx=5, pady=10)




        self.input_label = tk.Label(self.root,text="Account Number",bg="Gray", foreground="white", font=("Ariel", 12))
        
        self.input_label.grid(row=2, column=0,sticky="w", padx=5, pady=10)
       

        self.input_number = tk.Entry(self.root, width=30)
        self.input_number.grid(row=3, column=0,sticky="w", padx=5, pady=5)
    

        self.input_label2 = tk.Label(self.root, text="Amount:",bg="Gray",foreground="White",font=("Ariel", 12))
        self.input_label2.grid(row=4, column=0,sticky="w", padx=5, pady=0)


        self.input_number2 = tk.Entry(self.root, width=30)
        self.input_number2.grid(row=5, column=0,sticky="w", padx=5, pady=10)

        button_frame = tk.Frame(self.root, bg="gray")
        button_frame.grid(row=6, column=0, sticky="w", padx=5, pady=5, columnspan=3)

        self.publish = tk.Button(button_frame,text="Send", command=lambda:send(account, self.input_number, self.input_number2))
        self.publish.grid(row=6, column=0,sticky="w", padx=5, pady=0)


        self.refresh = tk.Button(button_frame,text="Refresh", command=lambda:self.update_balance(account))
        self.refresh.grid(row=6, column=1,sticky="w", padx=5, pady=0)


        self.random = tk.Button(button_frame, text = "Give money to each account", command=lambda:give_away(account))
        self.random.grid(row=6, column=2,sticky="w", padx=5, pady=0)

        self.root.grid_columnconfigure(0, weight=0)
        self.root.grid_columnconfigure(1, weight=0)
        self.root.grid_columnconfigure(2, weight=0)

 
        
        
        
        
        

    def update_balance(self,account):
        money = balance(account)
        self.label2.config(text=f'Balance: {money}')
    

    def run(self):
        self.root.mainloop()


run = UI()

run.run()
    





    



    


    


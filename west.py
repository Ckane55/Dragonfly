import pika, time,random
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import threading
import sqlite3


con = sqlite3.connect("Transactions.db")
cur = con.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS Transactions (Sender INTEGER , Reciever INTEGER, Amount INTEGER);""")
con.close()

#Initializing GUI
root = tk.Tk()

root.title("West transactions")

root.resizable(True,True)
#Make Windows two panes, one for each subscriber
paned = tk.PanedWindow(root, orient=tk.HORIZONTAL, sashrelief=tk.RAISED)

paned.pack(fill=tk.BOTH, expand=True)

#Left Panel
left_text = ScrolledText(paned, font=("Arial", 14))
paned.add(left_text)

#Right Panel
right_text = ScrolledText(paned, font=("Arial", 14))
paned.add(right_text)

left_text.insert(tk.END, "West Processor 1\nWaiting for transactions..." + "\n")
right_text.insert(tk.END, "West Processor 2\nWaiting for transactions..." + "\n")

#counter = 0


def new_balance(sender, reciever, amount):
    amount = int(amount)
    con = sqlite3.connect("Transactions.db", timeout=10)
    print("new_balance connection opened")
    cur = con.cursor()
    
    cur.execute("SELECT balance FROM Accounts WHERE account_number = ?", (sender,))
    new_sender_balance = cur.fetchone()
    temp = new_sender_balance[0]
    temp2 = temp - amount
    
    

    
    con.execute("BEGIN TRANSACTION")
    cur.execute("UPDATE Accounts SET balance = ? WHERE account_number = ?", (temp2,sender,))
    

    
    
    cur.execute("SELECT balance FROM Accounts WHERE account_number = ?", (reciever,))
    new_reciever_balance = cur.fetchone()
    temp = new_reciever_balance[0]
    temp3 = temp + amount
    
   

    
    cur.execute("UPDATE Accounts SET balance = ? WHERE account_number = ?", (temp3,reciever,))
    con.commit()
    con.close()
    print("new_balance connection closed")






#Consume() Runs in its own Thread
def consume():
    #Connecting to DB
    

   

    #Connecting to broker on local
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    #Initializing exchange
    #does nothing if exchange is already created
    channel.exchange_declare(exchange='card', exchange_type='topic')

    #Initializing Queue
    channel.queue_declare(queue='West',durable=True)


    #Binding queue to exchange with the topics 1000-1009
    #kinda clunky but I couldnt get it to work any other way
    for i in range(5000, 9999):
        channel.queue_bind(exchange='card', queue='West', routing_key=f"card.{i}")

    print(' [*] Waiting for logs. To exit press CTRL+C')

    

    #This function is called by pika everytime we recieve a message
    def callback(ch, method, properties, body):
       
       #Splitting up the topic to extract card number
       

       #splitting up body to extract counter variable and transaction amount
       split = body.decode()
       split = split.split(".")
       sender = int(split[0])
       reciever = int(split[1])
       amount = int(split[2])
       
       #Message displayed in GUI
       message = f"Transaction: From {sender} To: {reciever} Amount: ${amount}"
       
       #Displayed to right panel
       right_text.insert(tk.END, message + "\n")
       right_text.yview(tk.END)

       #Sleep function to simulate various processing times
       rand3 = random.randint(0,4)
       time.sleep(rand3)

       new_balance(sender, reciever,amount)

       #Store values in SQLite DB
       con = sqlite3.connect("Transactions.db",timeout = 10)
       print("Callback connection opened")
       cur = con.cursor()
       con.execute("BEGIN TRANSACTION")
       cur.execute("INSERT INTO Transactions VALUES(?,?,?)",(sender, reciever,amount))
       con.commit()
       con.close()
       print("Callback connection closed")

       #Send ACK to queue when processing is done so it can assign a new task
       ch.basic_ack(delivery_tag=method.delivery_tag)
       
    #Enables Fair Dispatch, if one processor is busy, send to other processor
    channel.basic_qos(prefetch_count=1)

    #Calls callback when a message is recieved in the West Queue
    channel.basic_consume(
        queue='West', on_message_callback=callback, auto_ack=False)

    #Try Finally function to close DB connection after all transactions have been processed
    #try:
    channel.start_consuming()
    #finally:
        #con.close()





        
def consume2():
    #Connecting to DB
    

   

    #Connecting to broker on local
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    #Initializing exchange
    #does nothing if exchange is already created
    channel.exchange_declare(exchange='card', exchange_type='topic')

    #Initializing Queue
    channel.queue_declare(queue='West',durable=True)


    #Binding queue to exchange with the topics 1000-1009
    #kinda clunky but I couldnt get it to work any other way
    for i in range(5000, 9999):
        channel.queue_bind(exchange='card', queue='West', routing_key=f"card.{i}")

    print(' [*] Waiting for logs. To exit press CTRL+C')

    

    #This function is called by pika everytime we recieve a message
    def callback(ch, method, properties, body):
       
       #Splitting up the topic to extract card number
       

       #splitting up body to extract counter variable and transaction amount
       split = body.decode()
       split = split.split(".")
       sender = int(split[0])
       reciever = int(split[1])
       amount = int(split[2])
       
       #Message displayed in GUI
       message = f"Transaction: From {sender} To: {reciever} Amount: ${amount}"
       
       #Displayed to right panel
       left_text.insert(tk.END, message + "\n")
       left_text.yview(tk.END)

       #Sleep function to simulate various processing times
       rand3 = random.randint(0,4)
       time.sleep(rand3)

       new_balance(sender, reciever,amount)

       #Store values in SQLite DB
       con = sqlite3.connect("Transactions.db",timeout = 10)
       print("Callback connection opened")
       cur = con.cursor()
       con.execute("BEGIN TRANSACTION")
       cur.execute("INSERT INTO Transactions VALUES(?,?,?)",(sender, reciever,amount))
       con.commit()
       con.close()
       print("Callback connection closed")

       #Send ACK to queue when processing is done so it can assign a new task
       ch.basic_ack(delivery_tag=method.delivery_tag)
       
    #Enables Fair Dispatch, if one processor is busy, send to other processor
    channel.basic_qos(prefetch_count=1)

    #Calls callback when a message is recieved in the East Queue
    channel.basic_consume(
        queue='West', on_message_callback=callback, auto_ack=False)

    #Try Finally function to close DB connection after all transactions have been processed
    
    channel.start_consuming()
  

    
#runs Consume() and Consume2() in their own threads
thread = threading.Thread(target=consume, daemon=True)
thread.start()


thread2 = threading.Thread(target=consume2, daemon=True)
thread2.start()

#Start GUI
root.mainloop()


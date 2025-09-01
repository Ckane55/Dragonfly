import pika, time,random
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import threading
import sqlite3

con = sqlite3.connect("Transactions.db")
cur = con.cursor()

cur.execute("DELETE FROM card_transaction")
con.commit()
con.close()

#Initializing GUI
root = tk.Tk()

root.title("West transactions")

root.resizable(True,True)
#Make Windows two panes, one for each subscriber
paned = tk.PanedWindow(root, orient=tk.HORIZONTAL, sashrelief=tk.RAISED)

paned.pack(fill=tk.BOTH, expand=True)

#Left Panel
left_text = ScrolledText(paned, font=("Arial", 20))
paned.add(left_text)

#Right Panel
right_text = ScrolledText(paned, font=("Arial", 20))
paned.add(right_text)

left_text.insert(tk.END, "West Processor 1\nWaiting for transactions..." + "\n")
right_text.insert(tk.END, "West Processor 2\nWaiting for transactions..." + "\n")

#counter = 0

#Consume() Runs in its own Thread
def consume():
    #Connecting to DB
    con = sqlite3.connect("Transactions.db")
    cur = con.cursor()

    
    
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
    for i in range(1010, 1019):
        channel.queue_bind(exchange='card', queue='West', routing_key=f"card.{i}")

    print(' [*] Waiting for logs. To exit press CTRL+C')

    

    #This function is called by pika everytime we recieve a message
    def callback(ch, method, properties, body):
       
       #Splitting up the topic to extract card number
       number = method.routing_key.split(".")

       #splitting up body to extract counter variable and transaction amount
       split = body.decode()
       split = split.split(".")
       counter = split[1]
       amount = split[0]
       
       #Message displayed in GUI
       message = f"{counter}. Card # ****{number[1]} Transaction:${amount}"
       
       #Displayed to right panel
       right_text.insert(tk.END, message + "\n")
       right_text.yview(tk.END)

       #Sleep function to simulate various processing times
       rand3 = random.randint(0,4)
       time.sleep(rand3)

       #Store values in SQLite DB
       cur.execute("INSERT INTO card_transaction VALUES(?,?)",(number[1], amount))
       con.commit()

       #Send ACK to queue when processing is done so it can assign a new task
       ch.basic_ack(delivery_tag=method.delivery_tag)
       
    #Enables Fair Dispatch, if one processor is busy, send to other processor
    channel.basic_qos(prefetch_count=1)

    #Calls callback when a message is recieved in the West Queue
    channel.basic_consume(
        queue='West', on_message_callback=callback, auto_ack=False)

    #Try Finally function to close DB connection after all transactions have been processed
    try:
        channel.start_consuming()
    finally:
        con.close()

#Consume2 runs the other processor
def consume2():
    #Connecting to DB
    con = sqlite3.connect("Transactions.db")
    cur = con.cursor()

    #Connecting to broker on local
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    #Initializing exchange
    #does nothing if exchange is already created
    channel.exchange_declare(exchange='card', exchange_type='topic')
    #Initializing Queue
    channel.queue_declare(queue='West',durable=True)


    #Binding queue to exchange with the topics 1010-1019
    #kinda clunky but I couldnt get it to work any other way
    for i in range(1010, 1019):
        channel.queue_bind(exchange='card', queue='West', routing_key=f"card.{i}")

    print(' [*] Waiting for logs. To exit press CTRL+C')

    

    #This function is called by pika everytime we recieve a message
    def callback(ch, method, properties, body):
       
       
      #Splitting up the topic to extract card number
       number = method.routing_key.split(".")
       
       #splitting up body to extract counter variable and transaction amount
       split = body.decode()
       split = split.split(".")
       counter = split[1]
       amount = split[0]

       #Message displayed in GUI
       message = f"{counter}. Card # ****{number[1]} Transaction:${amount}"
       
       #Displayed to left panel
       left_text.insert(tk.END, message + "\n")
       left_text.yview(tk.END)
       
       #Sleep function to simulate various processing times
       rand3 = random.randint(0,4)
       time.sleep(rand3)

       #Store values in SQLite DB
       cur.execute("INSERT INTO card_transaction VALUES(?,?)",(number[1], amount))
       con.commit()

       #Send ACK to queue when processing is done so it can assign a new task
       ch.basic_ack(delivery_tag=method.delivery_tag)
       
    #Enables Fair Dispatch, if one processor is busy, send to other processor
    channel.basic_qos(prefetch_count=1)

    #Calls callback when a message is recieved in the West Queue
    channel.basic_consume(
        queue='West', on_message_callback=callback, auto_ack=False)
    
    #Try Finally function to close DB connection after all transactions have been processed
    try:
        channel.start_consuming()
    finally:
        con.close()
    
#runs Consume() and Consume2() in their own threads
thread = threading.Thread(target=consume, daemon=True)
thread.start()

thread2 = threading.Thread(target=consume2, daemon=True)
thread2.start()

#Start GUI
root.mainloop()

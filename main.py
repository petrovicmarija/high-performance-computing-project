from mpi4py import MPI
from queue import Queue
import sys
import json


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    num_of_messages = 10
    queue1 = Queue(maxsize=5)
    queue2 = Queue(maxsize=5)

    # producer
    if rank == 0:
        for i in range(num_of_messages):
            try:
                routing_key = "queue1" if i % 2 == 0 else "queue2"
                message = {'message': i, 'routing_key': routing_key}
                comm.send(message, dest=1, tag=11)
                print("Producer sent message:", message)
            except MPI.Exception as e:
                print("Producer failed to send message to exchange:", e.error_string)
    # direct exchange
    elif rank == 1:
        for i in range(num_of_messages):
            try:
                message = comm.recv(source=0, tag=11)
                print("MESSAGE:", message)
                if message['routing_key'] == 'queue1':
                    comm.send(message, dest=2, tag=11)
                else:
                    comm.send(message, dest=3, tag=11)
            except MPI.Exception as e:
                print("Exchange failed to receive or send message to consumer(s):", e.error_string)
    # consumer 1
    elif rank == 2:
        for i in range(num_of_messages // 2):
            try:
                received_message = comm.recv(source=1, tag=11)
                queue1.put_nowait(received_message)
                print("Consumer 1 received message:", received_message)
                print("size1:", queue1.qsize())
                while not queue1.empty():
                    print("received 1")
                    print("[1]:", queue1.get_nowait())
                    if queue1.empty():
                        break
            except MPI.Exception as e:
                print("Consumer 1 failed to receive message from exchange:", e.error_string)
    # consumer 2
    elif rank == 3:
        for i in range(num_of_messages // 2):
            try:
                received_message = comm.recv(source=1, tag=11)
                queue2.put_nowait(received_message)
                print("Consumer 2 received message:", received_message)
                print("size2:", queue2.qsize())
                while not queue2.empty():
                    print("received 2")
                    print("[2]:", queue2.get_nowait())
                    if queue2.empty():
                        break
            except MPI.Exception as e:
                print("Consumer 2 failed to receive message from exchange:", e.error_string)


def main_user_input():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    queue1 = Queue(maxsize=5)
    queue2 = Queue(maxsize=5)

    # q1 = []
    # q2 = []
    # with open('consumer1.json') as json_file:
    #     data = json.load(json_file)
    #     q1 = data['queue1']
    #
    # with open('consumer2.json') as json_file:
    #     data = json.load(json_file)
    #     q2 = data['queue2']
    #
    # for msg in q1:
    #     queue1.put(msg)
    #
    # for msg in q2:
    #     queue2.put(msg)
    content = None
    routing_key = None
    # producer
    comm.Barrier()
    if rank == 0:
        for i in range(3):
            try:
                content = input("Enter message content: ")
                if content == "stop":
                    sys.exit("Producer stopped working.")
                routing_key = input("Enter routing key (options: queue1, queue2): ")
                message = {'message': content, 'routing_key': routing_key}
                comm.send(message, dest=1, tag=11)
                print("Producer sent message:", message)
            except MPI.Exception as e:
                print("Producer failed to send message to exchange:", e.error_string)
    comm.Barrier()
    # direct exchange
    if rank == 1:
        while True:
            try:
                message = comm.recv(source=0, tag=11)
                # print("MESSAGE:", message)
                if message['routing_key'] == 'queue1':
                    comm.send(message, dest=2, tag=11)
                elif message['routing_key'] == 'queue2':
                    comm.send(message, dest=3, tag=11)
            except MPI.Exception as e:
                print("Exchange failed to receive or send message to consumer(s):", e.error_string)
    # consumer 1
    elif rank == 2:
        while True:
            try:
                received_message = comm.recv(source=1, tag=11)
                queue1.put_nowait(received_message)
                print("Consumer 1 received message:", received_message)
                # print("size1:", queue1.qsize())
                while not queue1.empty():
                    # print("received 1")
                    print("[1]:", queue1.get_nowait())
                    if queue1.empty():
                        break
            except MPI.Exception as e:
                print("Consumer 1 failed to receive message from exchange:", e.error_string)
    # consumer 2
    elif rank == 3:
        while True:
            try:
                received_message = comm.recv(source=1, tag=11)
                queue2.put_nowait(received_message)
                print("Consumer 2 received message:", received_message)
                # print("size2:", queue2.qsize())
                while not queue2.empty():
                    # print("received 2")
                    print("[2]:", queue2.get_nowait())
                    if queue2.empty():
                        break
            except MPI.Exception as e:
                print("Consumer 2 failed to receive message from exchange:", e.error_string)


if __name__ == '__main__':
    # main()
    # file_c1 = open('consumer1.json')
    # data = json.load(file_c1)
    # for i in data['queue1']:
    #     print(i.content)
    #
    # file_c1.close()

    # consumer1_messages = []
    # with open('consumer1.json') as file:
    #     for message in file:
    #         message_dict = json.loads(message)
    #         consumer1_messages.append(message_dict)
    #
    # for msg in consumer1_messages:
    #     print(msg["content"])


    # file = open('consumer2.json', "r")
    # data = json.loads(file.read())
    # for i in data['queue2']:
    #     print(i)
    #
    # file.close()

    # q2 = []
    # with open('consumer2.json') as json_file:
    #     data = json.load(json_file)
    #
    #     print(data['queue2'])
    #     print(type(data['queue2']))
    #     q2 = data['queue2']
    #
    # for i in range(len(q2)):
    #     print(q2[i].get("content"))
    #     print(type(q2[i]))

    main_user_input()

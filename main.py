from mpi4py import MPI
from queue import Queue
import sys
import json
import time


def run_with_user_input():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    queue1 = Queue(maxsize=5)
    queue2 = Queue(maxsize=5)

    content = None
    routing_key = None
    # producer
    comm.Barrier()
    if rank == 0:
        for i in range(5):
            try:
                content = input("Enter message content: ")
                if content == "stop":
                    sys.exit("Producer stopped working.")
                routing_key = input("Enter routing key (options: q1, q2): ")
                message = {'message': content, 'routing_key': routing_key}
                req = comm.isend(message, dest=1, tag=11)
                req.wait()
                print("Producer sent message:", message)
            except MPI.Exception as e:
                print("Producer failed to send message to exchange:", e.error_string)
    comm.Barrier()
    # direct exchange
    if rank == 1:
        while True:
            try:
                req = comm.irecv(source=0, tag=11)
                message = req.wait()
                if message['routing_key'] == 'q1':
                    req = comm.isend(message, dest=2, tag=11)
                    req.wait()
                elif message['routing_key'] == 'q2':
                    req = comm.isend(message, dest=3, tag=11)
                    req.wait()
            except MPI.Exception as e:
                print("Exchange failed to receive or send message to consumer(s):", e.error_string)
    # consumer 1
    elif rank == 2:
        while True:
            try:
                req = comm.irecv(source=1, tag=11)
                received_message = req.wait()
                queue1.put_nowait(received_message)
                print("Consumer 1 received message:", received_message)
                while not queue1.empty():
                    print("[1]:", queue1.get_nowait())
                    time.sleep(3)
                    if queue1.empty():
                        break
            except MPI.Exception as e:
                print("Consumer 1 failed to receive message from exchange:", e.error_string)
    # consumer 2
    elif rank == 3:
        while True:
            try:
                req = comm.irecv(source=1, tag=11)
                received_message = req.wait()
                queue2.put_nowait(received_message)
                print("Consumer 2 received message:", received_message)
                while not queue2.empty():
                    print("[2]:", queue2.get_nowait())
                    time.sleep(3)
                    if queue2.empty():
                        break
            except MPI.Exception as e:
                print("Consumer 2 failed to receive message from exchange:", e.error_string)


def read_json(filename):
    with open(filename, "r") as file:
        data = json.load(file)
    return data


def save_to_json_for_consumer(filename, data):
    with open(filename, "w") as file:
        json.dump(data, file)


def run_with_files():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    queue1 = Queue(maxsize=5)
    queue2 = Queue(maxsize=5)

    content = None
    routing_key = None
    # producer
    comm.Barrier()
    if rank == 0:
        for i in range(5):
            try:
                content = input("Enter message content: ")
                if content == "stop":
                    sys.exit("Producer stopped working.")
                routing_key = input("Enter routing key (options: q1, q2): ")
                message = {'message': content, 'routing_key': routing_key}
                req = comm.isend(message, dest=1, tag=11)
                req.wait()
                print("Producer sent message:", message)
            except MPI.Exception as e:
                print("Producer failed to send message to exchange:", e.error_string)
    comm.Barrier()
    # direct exchange
    if rank == 1:
        while True:
            try:
                req = comm.irecv(source=0, tag=11)
                message = req.wait()
                if message['routing_key'] == 'q1':
                    req = comm.isend(message, dest=2, tag=11)
                    req.wait()
                elif message['routing_key'] == 'q2':
                    req = comm.isend(message, dest=3, tag=11)
                    req.wait()
            except MPI.Exception as e:
                print("Exchange failed to receive or send message to consumer(s):", e.error_string)
    # consumer 1
    elif rank == 2:
        while True:
            try:
                req = comm.irecv(source=1, tag=11)
                received_message = req.wait()
                queue1.put_nowait(received_message)
                data = read_json("consumer1.json")
                data.append(received_message)
                save_to_json_for_consumer("consumer1.json", data)
                print("Consumer 1 received message:", received_message)
                while not queue1.empty():
                    print("[1]:", queue1.get_nowait())
                    time.sleep(3)
                    if queue1.empty():
                        break
            except MPI.Exception as e:
                print("Consumer 1 failed to receive message from exchange:", e.error_string)
    # consumer 2
    elif rank == 3:
        while True:
            try:
                req = comm.irecv(source=1, tag=11)
                received_message = req.wait()
                queue2.put_nowait(received_message)
                data = read_json("consumer2.json")
                data.append(received_message)
                save_to_json_for_consumer("consumer2.json", data)
                print("Consumer 2 received message:", received_message)
                while not queue2.empty():
                    print("[2]:", queue2.get_nowait())
                    time.sleep(3)
                    if queue2.empty():
                        break
            except MPI.Exception as e:
                print("Consumer 2 failed to receive message from exchange:", e.error_string)


if __name__ == '__main__':
    # run_with_user_input()
    run_with_files()
from mpi4py import MPI
from queue import Queue


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


if __name__ == '__main__':
    main()

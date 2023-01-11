from mpi4py import MPI


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    queue1 = []
    queue2 = []
    num_of_messages = 10

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
                if message['routing_key'] == 'queue1':
                    # queue1.append(message)
                    comm.send(message, dest=2, tag=11)
                else:
                    # queue2.append(message)
                    comm.send(message, dest=3, tag=11)
            except MPI.Exception as e:
                print("Exchange failed to receive or send message to consumer(s):", e.error_string)
    # consumer 1
    elif rank == 2:
        for i in range(num_of_messages // 2):
            try:
                received_message = comm.recv(source=1, tag=11)
                queue1.append(received_message)
                print("Consumer 1 received message:", received_message)
            except MPI.Exception as e:
                print("Consumer 1 failed to receive message from exchange:", e.error_string)
    # consumer 2
    elif rank == 3:
        for i in range(num_of_messages // 2):
            try:
                received_message = comm.recv(source=1, tag=12)
                queue2.append(received_message)
                print("Consumer 2 received message:", received_message)
            except MPI.Exception as e:
                print("Consumer 2 failed to receive message from exchange:", e.error_string)


if __name__ == '__main__':
    main()

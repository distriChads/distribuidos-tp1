package rabbitmq

type RabbitWorker struct {
	Sender   *Sender
	Receiver *Receiver
}

func NewRabbitWorker(host string,
	queuesSend []string,
	callbacks map[string]func(string)) (*RabbitWorker, error) {
	sender, err := NewSender(host, queuesSend)
	if err != nil {
		return nil, err
	}
	receiver, err := NewReceiver(host, callbacks)
	if err != nil {
		return nil, err
	}
	return &RabbitWorker{Sender: sender, Receiver: receiver}, nil
}

func (r *RabbitWorker) StartListening() error {
	if err := r.Receiver.Run(); err != nil {
		return err
	}
	return nil
}

func (r *RabbitWorker) SendMessageTo(queue string, msg []byte) error {
	if err := r.Sender.SendTo(queue, msg); err != nil {
		return err
	}
	return nil
}

func (r *RabbitWorker) Close() {
	r.Sender.Close()
	r.Receiver.Close()
}

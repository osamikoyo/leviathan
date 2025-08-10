package retrier

func MultiConnect[T any](retriers, count uint, connector func() (T, error)) ([]T, error) {
	conns := make([]T, count)
	var err error

	for i := range retriers {
		conns[i], err = Connect(retriers, connector)
		if err != nil {
			return nil, err
		}
	}

	return conns, nil
}

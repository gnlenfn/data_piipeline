### TODO
1. 에러 원인 파악
```
ERROR:Traceback (most recent call last):
  File "/Users/hong/Documents/codes/spark_demo/venv/lib/python3.10/site-packages/h2/connection.py", line 224, in process_input
    func, target_state = self._transitions[(self.state, input_)]
KeyError: (<ConnectionState.CLOSED: 3>, <ConnectionInputs.RECV_PING: 14>)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/hong/Documents/codes/spark_demo/venv/lib/python3.10/site-packages/h2/connection.py", line 228, in process_input
    raise ProtocolError(
h2.exceptions.ProtocolError: Invalid input ConnectionInputs.RECV_PING in state ConnectionState.CLOSED

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/hong/Documents/codes/spark_demo/venv/lib/python3.10/site-packages/httpcore/_sync/http2.py", line 152, in handle_request
    raise LocalProtocolError(exc)  # pragma: nocover
httpcore.LocalProtocolError: Invalid input ConnectionInputs.RECV_PING in state ConnectionState.CLOSED

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/hong/Documents/codes/spark_demo/venv/lib/python3.10/site-packages/httpx/_transports/default.py", line 77, in map_httpcore_exceptions
    raise mapped_exc(message) from exc
```

    - h2? 안쓰는데
    - 어디서 나오는 에러인지도 모름
    - 
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
};

pub(crate) fn mock_http(responses: Vec<(u16, String)>) -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, receiver) = mpsc::channel();

    thread::spawn(move || {
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&stream);
            let _ = sender.send(request);

            let reason = if status == 200 { "OK" } else { "Error" };
            write!(
                stream,
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });

    (format!("http://{address}"), receiver)
}

fn read_request(stream: &TcpStream) -> String {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request = Vec::new();
    let mut content_length = 0;

    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(
            !line.is_empty(),
            "client closed before sending HTTP headers"
        );
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().unwrap();
        }
        request.extend_from_slice(line.as_bytes());
        if line == "\r\n" {
            break;
        }
    }

    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).unwrap();
    request.extend_from_slice(&body);
    String::from_utf8(request).unwrap()
}

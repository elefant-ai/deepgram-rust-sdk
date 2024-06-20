// TODO: Remove this lint
// Currently not documented because interface of this module is still changing
#![allow(missing_docs)]

//! Types used for live audio transcription.
//!
//! See the [Deepgram API Reference][api] for more info.
//!
//! [api]: https://developers.deepgram.com/api-reference/#transcription-streaming

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::channel::mpsc::{self, Receiver};
use futures::stream::StreamExt;
use futures::{SinkExt, Stream};
use http::Request;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::io::ReaderStream;
use tungstenite::handshake::client;
use url::Url;

use crate::{Deepgram, DeepgramError, Result};

use super::Transcription;

#[derive(Debug)]
pub struct StreamRequestBuilder<'a, S>
where
    S: Stream<Item = Bytes>,
{
    config: &'a Deepgram,
    source: Option<S>,
    encoding: Option<String>,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    keep_alive: bool,
    callback: Option<String>,
    model: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Word {
    pub word: String,
    pub start: f64,
    pub end: f64,
    pub confidence: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Alternatives {
    pub transcript: String,
    pub words: Vec<Word>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Channel {
    pub alternatives: Vec<Alternatives>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StreamResponse {
    TranscriptResponse {
        duration: f64,
        is_final: bool,
        channel: Channel,
    },
    TerminalResponse {
        request_id: String,
        created: String,
        duration: f64,
        channels: u32,
    },
}

#[pin_project]
struct FileChunker {
    chunk_size: usize,
    buf: BytesMut,
    #[pin]
    file: ReaderStream<File>,
}

impl Transcription<'_> {
    pub fn stream_request<S: Stream<Item = Bytes>>(&self) -> StreamRequestBuilder<S> {
        StreamRequestBuilder {
            config: self.0,
            source: None,
            encoding: None,
            sample_rate: None,
            channels: None,
            keep_alive: false,
            callback: None,
            model: None,
        }
    }
}

impl FileChunker {
    fn new(file: File, chunk_size: usize) -> Self {
        FileChunker {
            chunk_size,
            buf: BytesMut::with_capacity(2 * chunk_size),
            file: ReaderStream::new(file),
        }
    }
}

impl Stream for FileChunker {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        while this.buf.len() < *this.chunk_size {
            match Pin::new(&mut this.file).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(next) => match next.transpose() {
                    Err(e) => return Poll::Ready(Some(Err(DeepgramError::from(e)))),
                    Ok(None) => {
                        if this.buf.is_empty() {
                            return Poll::Ready(None);
                        } else {
                            return Poll::Ready(Some(Ok(this
                                .buf
                                .split_to(this.buf.len())
                                .freeze())));
                        }
                    }
                    Ok(Some(next)) => {
                        this.buf.extend_from_slice(&next);
                    }
                },
            }
        }

        Poll::Ready(Some(Ok(this.buf.split_to(*this.chunk_size).freeze())))
    }
}

impl<'a, S> StreamRequestBuilder<'a, S>
where
    S: Stream<Item = Bytes>,
{
    pub fn stream(mut self, stream: S) -> Self {
        self.source = Some(stream);

        self
    }

    pub fn encoding(mut self, encoding: String) -> Self {
        self.encoding = Some(encoding);

        self
    }

    pub fn sample_rate(mut self, sample_rate: u32) -> Self {
        self.sample_rate = Some(sample_rate);

        self
    }

    pub fn channels(mut self, channels: u16) -> Self {
        self.channels = Some(channels);

        self
    }

    pub fn callback(mut self, callback: String) -> Self {
        self.callback = Some(callback);

        self
    }

    pub fn keep_alive(mut self) -> Self {
        self.keep_alive = true;
        self
    }

    pub fn model(mut self, model: String) -> Self {
        self.model = Some(model);

        self
    }
}

impl<'a> StreamRequestBuilder<'a, Receiver<Bytes>> {
    pub async fn file(
        mut self,
        filename: impl AsRef<Path>,
        frame_size: usize,
        frame_delay: Duration,
    ) -> Result<StreamRequestBuilder<'a, Receiver<Bytes>>> {
        let file = File::open(filename).await?;
        let mut chunker = FileChunker::new(file, frame_size);
        let (mut tx, rx) = mpsc::channel(1);
        let task = async move {
            while let Some(frame) = chunker.next().await {
                tokio::time::sleep(frame_delay).await;
                // This unwrap() is safe because application logic dictates that the Receiver won't
                // be dropped before the Sender.
                match frame {
                    Ok(frame) => tx.send(frame).await.unwrap(),
                    Err(e) => {
                        let _ = dbg!(e);
                        break;
                    }
                }
            }
        };
        tokio::spawn(task);

        self.source = Some(rx);
        Ok(self)
    }
}

impl<S> StreamRequestBuilder<'_, S>
where
    S: Stream<Item = Bytes> + Send + Unpin + 'static,
{
    pub async fn start(self) -> Result<Receiver<Result<StreamResponse>>> {
        let StreamRequestBuilder {
            config,
            source,
            encoding,
            sample_rate,
            channels,
            keep_alive,
            callback,
            model,
        } = self;
        let mut source = source
            .ok_or(DeepgramError::NoSource)?
            .map(|bytes| Message::binary(Vec::from(bytes.as_ref())));

        // This unwrap is safe because we're parsing a static.
        let mut base = Url::parse("wss://api.deepgram.com/v1/listen").unwrap();
        {
            let mut pairs = base.query_pairs_mut();
            if let Some(encoding) = encoding {
                pairs.append_pair("encoding", &encoding);
            }
            if let Some(sample_rate) = sample_rate {
                pairs.append_pair("sample_rate", &sample_rate.to_string());
            }
            if let Some(channels) = channels {
                pairs.append_pair("channels", &channels.to_string());
            }
            if let Some(callback) = callback {
                pairs.append_pair("callback", &callback);
            }
            if let Some(model) = model {
                pairs.append_pair("model", &model);
            }
        }

        let request = Request::builder()
            .method("GET")
            .uri(base.to_string())
            .header("authorization", format!("token {}", config.api_key))
            .header("sec-websocket-key", client::generate_key())
            .header("host", "api.deepgram.com")
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .body(())?;
        let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let (mut tx, rx) = mpsc::channel::<Result<StreamResponse>>(1);

        let send_task = async move {
            loop {
                if keep_alive {
                    let res = tokio::select! {
                        res = source.next() => Some(res),
                        _ = tokio::time::sleep(Duration::from_secs(8)) => None,
                    };
                    if let Some(res) = res {
                        match res {
                            None => break,
                            Some(frame) => {
                                // This unwrap is not safe.
                                write.send(frame).await.unwrap();
                            }
                        }
                    } else {
                        // This unwrap is not safe.
                        write
                            .send(Message::text("{\"type\":\"KeepAlive\"}"))
                            .await
                            .unwrap();
                    }
                } else {
                    match source.next().await {
                        None => break,
                        Some(frame) => {
                            // This unwrap is not safe.
                            write.send(frame).await.unwrap();
                        }
                    }
                }
            }

            // This unwrap is not safe.
            write.send(Message::binary([])).await.unwrap();
        };

        let recv_task = async move {
            loop {
                match read.next().await {
                    None => break,
                    Some(Ok(msg)) => {
                        if let Message::Text(txt) = msg {
                            let resp = serde_json::from_str(&txt).map_err(DeepgramError::from);
                            tx.send(resp)
                                .await
                                // This unwrap is probably not safe.
                                .unwrap();
                        }
                    }
                    Some(e) => {
                        let _ = dbg!(e);
                        break;
                    }
                }
            }
        };

        tokio::spawn(async move {
            tokio::join!(send_task, recv_task);
        });

        Ok(rx)
    }
}

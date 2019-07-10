use crate::{
    buffers::Acker,
    event::Event,
    sinks::util::{
        encoding::BasicEncoding,
        file::{EmbeddedFileSink, FileSink},
        SinkExt,
    },
    template::Template,
    topology::config::DataType,
};

use futures::{future, Async, AsyncSink, Sink, StartSend};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileSinkConfig {
    #[serde(rename = "path")]
    pub path_template: String,
    #[serde(rename = "timeout", default = "default_close_timeout_secs")]
    pub close_timeout_secs: Option<u64>,
    pub encoding: Option<BasicEncoding>,
}

fn default_close_timeout_secs() -> Option<u64> {
    None //means that we don't close files, so no overhead
}

impl FileSinkConfig {
    pub fn new(path_template: String) -> Self {
        Self {
            path_template,
            close_timeout_secs: default_close_timeout_secs(),
            encoding: None,
        }
    }
}

#[typetag::serde(name = "file")]
impl crate::topology::config::SinkConfig for FileSinkConfig {
    fn build(&self, acker: Acker) -> Result<(super::RouterSink, super::Healthcheck), String> {
        let sink = PartitionedFileSink::new(
            Template::from(&self.path_template[..]),
            self.close_timeout_secs,
            self.encoding.clone(),
        )
        .stream_ack(acker);

        Ok((Box::new(sink), Box::new(future::ok(()))))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }
}

pub struct PartitionedFileSink {
    path_template: Template,
    encoding: Option<BasicEncoding>,
    close_timeout_secs: Option<u64>,

    partitions: HashMap<PathBuf, EmbeddedFileSink>,
    last_accessed: HashMap<PathBuf, Instant>,
    closing: Vec<EmbeddedFileSink>,
}

impl PartitionedFileSink {
    pub fn new(
        path_template: Template,
        close_timeout_secs: Option<u64>,
        encoding: Option<BasicEncoding>,
    ) -> Self {
        PartitionedFileSink {
            path_template,
            encoding,
            close_timeout_secs,
            partitions: HashMap::new(),
            last_accessed: HashMap::new(),
            closing: Vec::new(),
        }
    }

    fn collect_old_files(&mut self) {
        let mut recently_outdated = vec![];
        for timeout in self.close_timeout_secs {
            self.last_accessed.retain(|path, time| {
                if time.elapsed().as_secs() > timeout {
                    debug!(message = "removing sink", file = ?path);
                    recently_outdated.push(path.clone());
                    false
                } else {
                    true
                }
            });
        }

        let mut recently_outdated = recently_outdated
            .into_iter()
            .map(|ref path| self.partitions.remove(path).unwrap())
            .collect();

        //it is easier to empty `closing` buffer and put back not closed sinks, see `poll_close`
        let mut closing: Vec<EmbeddedFileSink> = self.closing.drain(..).collect();
        closing.append(&mut recently_outdated);

        for file in closing.into_iter() {
            self.poll_close(file);
        }
    }

    fn poll_close(&mut self, mut file: EmbeddedFileSink) {
        match file.close() {
            Err(err) => error!("Error while closing FileSink: {:?}", err),
            Ok(Async::Ready(())) => debug!("a FileSink closed"),
            Ok(Async::NotReady) => self.closing.push(file),
        }
    }
}

impl Sink for PartitionedFileSink {
    type SinkItem = Event;
    type SinkError = ();

    fn start_send(&mut self, event: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self.path_template.render(&event) {
            Ok(bytes) => {
                let path = PathBuf::from(String::from_utf8_lossy(&bytes).as_ref());

                let partition = self
                    .partitions
                    .entry(path.clone())
                    .or_insert(FileSink::new_with_encoding(&path, self.encoding.clone()));

                self.last_accessed.insert(path.clone(), Instant::now());
                partition.start_send(event)
            }

            Err(missing_keys) => {
                warn!(
                    message = "Keys do not exist on the event. Dropping event.",
                    keys = ?missing_keys
                );
                Ok(AsyncSink::Ready)
            }
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        self.partitions
            .iter_mut()
            .for_each(|(path, partition)| match partition.poll_complete() {
                Ok(_) => {}
                Err(()) => error!("Error in downstream FileSink with path {:?}", path),
            });

        self.collect_old_files();

        let files: Vec<PathBuf> = self.partitions.keys().map(|path| path.clone()).collect();
        debug!(message = "keeping opened", files = ?files);

        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        buffers::Acker,
        event,
        test_util::{lines_from_file, random_events_with_stream, random_lines_with_stream},
        topology::config::SinkConfig,
    };

    use core::convert::From;

    use futures::stream;
    use tempfile::tempdir;

    #[test]
    fn without_partitions() {
        let directory = tempdir().unwrap();

        let mut template = directory.into_path().to_string_lossy().to_string();
        template.push_str("/test.out");

        let config = FileSinkConfig::new(template.clone());

        let (sink, _) = config.build(Acker::Null).unwrap();
        let (input, events) = random_lines_with_stream(100, 64);

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let pump = sink.send_all(events);
        let _ = rt.block_on(pump).unwrap();

        let output = lines_from_file(template);
        for (input, output) in input.into_iter().zip(output) {
            assert_eq!(input, output);
        }
    }

    #[test]
    fn partitions_are_created_dynamically() {
        let directory = tempdir().unwrap();
        let directory = directory.into_path();

        let mut template = directory.to_string_lossy().to_string();
        template.push_str("/{{level}}s-{{date}}.log");

        let config = FileSinkConfig::new(template.clone());

        let (sink, _) = config.build(Acker::Null).unwrap();

        let (mut input, _) = random_events_with_stream(32, 8);
        input[0]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-26-07".into());
        input[0]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[1]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-26-07".into());
        input[1]
            .as_mut_log()
            .insert_implicit("level".into(), "error".into());
        input[2]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-26-07".into());
        input[2]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[3]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-27-07".into());
        input[3]
            .as_mut_log()
            .insert_implicit("level".into(), "error".into());
        input[4]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-27-07".into());
        input[4]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[5]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-27-07".into());
        input[5]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[6]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-28-07".into());
        input[6]
            .as_mut_log()
            .insert_implicit("level".into(), "warning".into());
        input[7]
            .as_mut_log()
            .insert_implicit("date".into(), "2019-29-07".into());
        input[7]
            .as_mut_log()
            .insert_implicit("level".into(), "error".into());

        let events = stream::iter_ok(input.clone().into_iter());
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let pump = sink.send_all(events);
        let _ = rt.block_on(pump).unwrap();

        let output = vec![
            lines_from_file(&directory.join("warnings-2019-26-07.log")),
            lines_from_file(&directory.join("errors-2019-26-07.log")),
            lines_from_file(&directory.join("warnings-2019-27-07.log")),
            lines_from_file(&directory.join("errors-2019-27-07.log")),
            lines_from_file(&directory.join("warnings-2019-28-07.log")),
            lines_from_file(&directory.join("errors-2019-29-07.log")),
        ];

        assert_eq!(
            input[0].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[0][0])
        );
        assert_eq!(
            input[1].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[1][0])
        );
        assert_eq!(
            input[2].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[0][1])
        );
        assert_eq!(
            input[3].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[3][0])
        );
        assert_eq!(
            input[4].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[2][0])
        );
        assert_eq!(
            input[5].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[2][1])
        );
        assert_eq!(
            input[6].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[4][0])
        );
        assert_eq!(
            input[7].as_log()[&event::MESSAGE],
            From::<&str>::from(&output[5][0])
        );
    }
}

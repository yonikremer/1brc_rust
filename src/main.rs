use std::{str, thread};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::str::Utf8Error;
use std::thread::ScopedJoinHandle;

use file_chunker::FileChunker;

const FILE_PATH: &str = "data/measurements.txt";
const NUM_THREADS: usize = 1000000;

#[derive(Clone)]
struct CityInfo{
    max_temp: f32,
    min_temp: f32,
    num_measurements: u32,
    sum_measurements: f32
}


type CitiesMap = HashMap<String, CityInfo>;

impl CityInfo{
    fn new(first_measurement: f32) -> CityInfo{
        CityInfo{
            max_temp: first_measurement,
            min_temp: first_measurement,
            num_measurements: 1,
            sum_measurements: first_measurement
        }
    }

    fn add_measurement(&mut self, new_measurement: f32) {
        if new_measurement > self.max_temp {self.max_temp = new_measurement }
        else if new_measurement < self.min_temp { self.min_temp = new_measurement }
        self.num_measurements += 1;
        self.sum_measurements += new_measurement;
    }

    fn merge(&mut self, other: &CityInfo) {
        self.max_temp = if self.max_temp > other.max_temp {self.max_temp} else {other.max_temp};
        self.min_temp = if self.min_temp < other.min_temp {self.min_temp} else {other.min_temp};
        self.num_measurements += other.num_measurements;
        self.sum_measurements += other.sum_measurements;
    }
}


impl Display for CityInfo{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.min_temp, self.sum_measurements / (self.num_measurements as f32), self.max_temp)
    }
}


fn print_results(result_maps: Vec<CitiesMap>) -> (){
    let mut result = HashMap::<String, CityInfo>::new();
    for curr_map in result_maps.iter(){
        for (city_name, value) in curr_map.iter(){
            if let Some(result_city_info) = result.get_mut(city_name){
                result_city_info.merge(value);
            }
            else{
                result.insert(String::from(city_name), value.clone());
            }
        }
    }
    print!("{}", "{");
    for (key, value) in result.iter(){
        print!("{}={}, ", key, value);
    }
}


fn process_line(line: &str, map: &mut CitiesMap) {
    // This function gets a line from the measurements file and adds it to the map
    // Assumes: The line has a city name and a temperature string.
    // The temperature string is a decimal number with 1 digit.
    // The city name is stronger than the temperature string
    let (city_name, temp_string): (&str, &str) = line.split_once(';')
        .expect(format!("Can't find ';' in {}", line).as_str());
    let temp: f32 = temp_string.parse::<f32>().unwrap();
    if let Some(city_info) = map.get_mut(city_name) {
        city_info.add_measurement(temp);
    } else {
        map.insert(String::from(city_name), CityInfo::new(temp));
    }
}


fn process_chunk(chunk: &[u8]) -> Result<CitiesMap, Utf8Error>{
    let mut map: CitiesMap = HashMap::default();
    let mut start_index = 0;
    for (i, &byte) in chunk.iter().enumerate() {
        if byte == b'\n' {
            match str::from_utf8(&chunk[start_index..i]) {
                Ok(line) => process_line(line, &mut map),
                Err(error) => {
                    eprintln!("Invalid UTF-8 sequence in chunk");
                    return Err(error);
                }
            }
            start_index = i + 1;
        }
    }
    Ok(map)
}


fn main() {
    let file_path = FILE_PATH;
    let file = match File::open(file_path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("Failed to open file {}: {}", file_path, err);
            return;
        }
    };
    let chunker = match FileChunker::new(&file) {
        Ok(chunker) => chunker,
        Err(err) => {
            eprintln!("Failed to create chunker: {}", err);
            return;
        }
    };
    thread::scope(|s| {
    // Spawn threads to process chunks and collect results
        let chunks: Vec<&[u8]> = chunker.chunks(1_000_000_000 / NUM_THREADS, Some('\n')).unwrap();
        let result_maps: Vec<ScopedJoinHandle<CitiesMap>> = chunks.into_iter()
            .map(|chunk: &[u8]| s.spawn(move || {
                process_chunk(chunk).unwrap()
            }))
            .collect();
        // Collect results from threads
        let collected_results: Vec<CitiesMap> = result_maps
            .into_iter()
            .map(|handle| handle.join())
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_default();
        print_results(collected_results);
    })
}


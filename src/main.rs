extern crate scoped_threadpool;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::str;
use std::str::Utf8Error;
use std::sync::{Arc, Mutex, MutexGuard};

use file_chunker::FileChunker;
use scoped_threadpool::{Pool, Scope};

const FILE_PATH: &str = "data/measurements.txt";
const NUM_THREADS: usize = 16;
const CHUNK_SIZE: usize = 10000;

#[derive(Clone)]
struct CityInfo{
    max_temp: f32,
    min_temp: f32,
    num_measurements: u32,
    sum_measurements: f32
}


type CitiesMap = HashMap<String, CityInfo>;
type CitiesMaps = Vec<CitiesMap>;
type ThreadSafeCitiesMaps = Arc<Mutex<CitiesMaps>>;

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


fn print_results(result_maps: ThreadSafeCitiesMaps) -> (){
    let mut result: CitiesMap = HashMap::<String, CityInfo>::new();
    for curr_map in result_maps.lock().unwrap().iter() {
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
    let file: File = match File::open(FILE_PATH) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("Failed to open file {}: {}", FILE_PATH, err);
            return;
        }
    };
    let chunker: FileChunker = FileChunker::new(&file).expect("Failed to create chunker");
    let mut pool: Pool = Pool::new(NUM_THREADS as u32);
    let chunks: Vec<&[u8]> = chunker.chunks(CHUNK_SIZE, Some('\n')).unwrap();

    let result_maps: ThreadSafeCitiesMaps = Arc::new(Mutex::new(Vec::new()));
    
    pool.scoped(|s: &Scope| {
        for chunk in chunks {
            let result_maps_clone: ThreadSafeCitiesMaps = Arc::clone(&result_maps);
            s.execute(move || {
                let result: CitiesMap = process_chunk(&chunk).unwrap();
                let mut result_maps: MutexGuard<CitiesMaps> = result_maps_clone.lock().unwrap();
                result_maps.push(result);
                println!("processed {} chunks", result_maps.len());
            });
        }
    });
    
    print_results(result_maps);
}


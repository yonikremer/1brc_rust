use std::{str, thread};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::RandomState;
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::thread::ScopedJoinHandle;

use file_chunker::FileChunker;

const FILE_PATH: &str = "data/measurements.txt";
const NUM_THREADS: usize = 16;

#[derive(Clone)]
struct CityInfo{
    max_temp: i16,
    min_temp: i16,
    num_measurements: u16,
    sum_measurements: i32
}


impl CityInfo{
    fn new(first_measurement: i16) -> CityInfo{
        CityInfo{
            max_temp: first_measurement,
            min_temp: first_measurement,
            num_measurements: 1,
            sum_measurements: first_measurement as i32
        }
    }

    fn add_measurement(&mut self, new_measurement: i16) {
        if new_measurement > self.max_temp {self.max_temp = new_measurement }
        else if new_measurement < self.min_temp { self.min_temp = new_measurement }
        self.num_measurements += 1;
        self.sum_measurements += new_measurement as i32;
    }

    fn merge(&mut self, other: &CityInfo) {
        self.max_temp = max(self.max_temp, other.max_temp);
        self.min_temp = min(self.min_temp, other.min_temp);
        self.num_measurements += other.num_measurements;
        self.sum_measurements += other.sum_measurements;
    }
}


impl Display for CityInfo{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let min: f32 = self.min_temp as f32 / 10.0;
        let max: f32 = self.max_temp as f32 / 10.0;
        let mean: f32 = self.sum_measurements as f32 / (self.sum_measurements as f32 * 10.0);
        write!(f, "{min}/{mean}/{max}")
    }
}


fn decimal_str_to_int<'a>(decimal_str: String) -> Result<i16, ParseIntError>{
    let mut split = decimal_str.split(".");
    let before_dot: i16 = split.next().unwrap().parse::<i16>()?;
    let after_dot: i16 = if let Some(str_after_dot) = split.next(){
        str_after_dot.parse::<i16>()?
    } else{
        0
    };
    return Ok(before_dot * 10 + after_dot);
}


fn print_results(result_maps: Vec<HashMap<String, CityInfo>>) -> (){
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


fn process_line(line: &str, map: &mut HashMap<String, CityInfo>) {
    let semicolon_index = line.rfind(";").unwrap();
    let city_name: &str = &line[..semicolon_index];
    let temp_string: &str = &line[semicolon_index + 1..];
    let temp_int: i16 = decimal_str_to_int(temp_string.to_string()).unwrap();
    if let Some(city_info) = map.get_mut(city_name) {
        city_info.add_measurement(temp_int);
    } else {
        map.insert(String::from(city_name), CityInfo::new(temp_int));
    }
}


fn process_chunk(chunk: &[u8]) -> Result<HashMap<String, CityInfo>, Utf8Error>{
    let mut map: HashMap<String, CityInfo> = HashMap::new();
    let result = str::from_utf8(chunk);
    return if let Err(error) = result {
        Err(error)
    } else {
        let chunk_string = result.unwrap();
        for line in chunk_string.split("\n") {
            process_line(line, &mut map);
        }
        Ok(map)
    }
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
        let result_maps: Vec<ScopedJoinHandle<HashMap<String, CityInfo, RandomState>>> = match chunker.chunks(1_000_000_000 / NUM_THREADS, Some('\n')) {
            Ok(chunks) => chunks
                .into_iter()
                .map(|chunk| s.spawn(move || { 
                    process_chunk(chunk).expect("Found invalid UTF-8 chunk") 
                }))
                .collect(),
            Err(err) => {
                eprintln!("Failed to chunk file: {}", err);
                return;
            }
        };
        // Collect results from threads
        let collected_results: Vec<HashMap<String, CityInfo>> = result_maps
            .into_iter()
            .map(|handle| handle.join())
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_default();
        print_results(collected_results);
    })
}


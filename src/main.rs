use std::{str, thread};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
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
    if let None = decimal_str.find(".") {
        panic!("Invalid decimal string {}", decimal_str);
    }
    let mut split = decimal_str.split(".");
    let before_dot: i16 = split.next().expect(format!("Decimal String: {} doesn't have any dots!", decimal_str).as_str()).parse::<i16>()?;
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
    if line.len() < 5{
        eprintln!("Line is too short!, Line: {}", line);
        return;
    }
    if let None = line.find(";") {
        eprintln!("Invalid line {}", line);
        return;
    }
    if let None = line.find(".") {
        eprintln!("Invalid line {}", line);
        return;
    }
    let semicolon_index: usize = line.rfind(";").expect(format!("Line {} doesn't have a semicolon", line).as_str());
    let city_name: &str = &line[..semicolon_index];
    let temp_string: &str = &line[semicolon_index + 1..];
    let temp_int: i16 = decimal_str_to_int(temp_string.to_string())
        .expect(format!("Can't parse {} as a number", temp_string.to_string()).as_str());
    if let Some(city_info) = map.get_mut(city_name) {
        city_info.add_measurement(temp_int);
    } else {
        map.insert(String::from(city_name), CityInfo::new(temp_int));
    }
}


fn process_chunk(chunk: &[u8]) -> Result<HashMap<String, CityInfo>, Utf8Error>{
    let mut map: HashMap<String, CityInfo> = HashMap::new();
    let mut start_index = 0;
    for (i, &byte) in chunk.iter().enumerate() {
        if byte == b'\n' {
            if let Ok(line) = str::from_utf8(&chunk[start_index..i]) {
                process_line(line, &mut map);
            } else {
                eprintln!("Invalid UTF-8 sequence in chunk");
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
        let result_maps: Vec<ScopedJoinHandle<HashMap<String, CityInfo>>> = match chunker.chunks(1_000_000_000 / NUM_THREADS, Some('\n')) {
            Ok(chunks) => chunks
                .into_iter()
                .map(|chunk| s.spawn(move || { 
                    match process_chunk(chunk){
                        Ok(my_map) => my_map,
                        Err(err) => {
                            println!("Chunk: {}", String::from_utf8_lossy(chunk));
                            panic!("Failed to read chunk as a UTF-8 string: {}", err);
                        }
                    }
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


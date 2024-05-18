use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::path::Path;
use std::str;
use std::sync::{Arc, Mutex, MutexGuard};

use clap::Parser;
use clap_derive::Parser;
use file_chunker::FileChunker;
use scoped_threadpool::{Pool, Scope};

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
    let mut result_iter: Iter<String, CityInfo> = result.iter();
    let (first_key, first_value) = result_iter.next().expect("result is empty");
    print!("{}={}", first_key, first_value);
    for (key, value) in result_iter{
        print!(", {}={}", key, value);
    }
    println!("{}", "}");
}


fn process_line(line: &str, map: &mut CitiesMap) -> Result<(), Box<dyn std::error::Error>> {
    // This function gets a line from the measurements file and adds it to the map
    // Assumes: The line has a city name and a temperature string.
    // The temperature string is a decimal number with 1 digit.
    // The city name is stronger than the temperature string
    // let (city_name, temp_string): (&str, &str) = line.split_once(';')
    //     .expect(format!("Can't find ';' in {}", line).as_str());
    if let Some((city_name, temp_string)) = line.split_once(';') {
        let city_name = city_name.trim();
        if city_name.is_empty() {
            let error_string = format!("The city name is empty: {}", line);
            eprintln!("{}", error_string);
            return Err(error_string.into());
        }
        let temp_string = temp_string.trim();
        if temp_string.is_empty() {
            let error_string = format!("The temperature string is empty: {}", temp_string);
            eprintln!("{}", error_string);
            return Err(error_string.into());
        }
        if let Ok(temp) = temp_string.parse::<f32>(){
            if let Some(city_info) = map.get_mut(city_name) {
                city_info.add_measurement(temp);
            } else {
                map.insert(String::from(city_name), CityInfo::new(temp));
            }
            Ok(())
        }
        else{
            let error_string = format!("The temperature string is not a number: {}", line);
            eprintln!("{}", error_string);
            Err(error_string.into())
        }
    }
    else{
        let error_string = format!("Can't find ';' in {}", line);
        eprintln!("{}", error_string);
        Err(error_string.into())
    }
    
}


fn process_chunk(chunk: &[u8]) -> Result<CitiesMap, Box<dyn std::error::Error>>{
    let mut map: CitiesMap = HashMap::default();
    let mut start_index = 0;
    for (i, &byte) in chunk.iter().enumerate() {
        if byte == b'\n' {
            match str::from_utf8(&chunk[start_index..i]) {
                Ok(line) => { 
                    process_line(line, &mut map)?;
                },
                Err(error) => {
                    eprintln!("Invalid UTF-8 sequence in chunk");
                    eprintln!("The invalid sequence is {:?}", &chunk[start_index..i]);
                    eprintln!("The error is {:?}", error);
                    return Err(Box::new(error));
                }
            }
            start_index = i + 1;
        }
    }
    Ok(map)
}


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the file
    #[arg(short, long)]
    file_path: String,

    /// Number of lines_per_chunk
    #[arg(short, long, default_value_t = 256)]
    lines_per_chunk: usize,
}


// Validate the arguments to check that the file exists and that the lines_per_chunk is > 0
impl Args {
    fn validate(&self) -> Result<(), String> {
        if self.lines_per_chunk == 0 {
            return Err(format!("Lines per chunk must be > 0. Got {}", self.lines_per_chunk));
        }
        if !Path::new(&self.file_path).exists() {
            return Err(format!("File {} does not exist", self.file_path));
        }
        Ok(())
    }
}


fn main() {
    let args: Args = Args::parse();
    match args.validate() {
        Ok(_) => {},
        Err(err) => {
            eprintln!("{}", err);
            return;
        }
    }
    let file: File = match File::open(&args.file_path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("Failed to open file {}: {}", &args.file_path, err);
            return;
        }
    };
    let chunker: FileChunker = FileChunker::new(&file).expect("Failed to create chunker");
    let mut pool: Pool = Pool::new(num_cpus::get() as u32);
    let chunks: Vec<&[u8]> = chunker.chunks(args.lines_per_chunk, Some('\n')).expect("Failed to get chunks");
    let result_maps: ThreadSafeCitiesMaps = Arc::new(Mutex::new(Vec::new()));
    pool.scoped(|s: &Scope| {
        for chunk in chunks {
            let result_maps_clone: ThreadSafeCitiesMaps = Arc::clone(&result_maps);
            s.execute(move || {
                let result: CitiesMap = process_chunk(&chunk).unwrap();
                let mut result_maps: MutexGuard<CitiesMaps> = result_maps_clone.lock().expect("Failed to lock mutex");
                result_maps.push(result);
            });
        }
    });
    print_results(result_maps);
}

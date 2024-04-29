use std::{fs};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::{BufRead, BufReader};
use std::num::ParseIntError;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};
use threadpool::ThreadPool;

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


fn print_results(result_maps: Vec<Arc<Mutex<HashMap<String, CityInfo>>>>) -> (){
    let mut result = HashMap::<String, CityInfo>::new();
    for curr_map_arc in result_maps.iter(){
        let arc_clone = curr_map_arc.clone();
        for (city_name, value) in arc_clone.deref().lock().unwrap().deref().iter(){
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



fn main() {
    let file = fs::File::open(FILE_PATH).unwrap();
    let reader = BufReader::new(file);
    // Create a vector of mutex-protected hash maps (one for each thread)
    let result_maps: Vec<Arc<Mutex<HashMap<String, CityInfo>>>> = (0..NUM_THREADS)
        .map(|_| Arc::new(Mutex::new(HashMap::new())))
        .collect();

    let pool = ThreadPool::new(NUM_THREADS);

    for (thread_id, line) in reader.lines().enumerate() {
        let line = line.expect("Error reading line");
        let result_map_clone = result_maps[thread_id % NUM_THREADS].clone();
        pool.execute(move || {
            process_line(&line, &result_map_clone);
        });
    }

    print_results(result_maps);
}

fn process_line(line: &String, map: &Arc<Mutex<HashMap<String, CityInfo>>>) {
    let semicolon_index = line.rfind(";").unwrap();
    let city_name: &str = &line[..semicolon_index];
    let temp_string: &str = &line[semicolon_index + 1..];
    let temp_int: i16 = decimal_str_to_int(temp_string.to_string()).unwrap();
    let mut guard: MutexGuard<HashMap<String, CityInfo>> = map.deref().lock().expect("An error getting the lock to the map");
    let map_value: &mut HashMap<String, CityInfo> = guard.deref_mut();
    if let Some(city_info) = map_value.get_mut(city_name) {
        city_info.add_measurement(temp_int);
    } else {
        map_value.insert(String::from(city_name), CityInfo::new(temp_int));
    }
}

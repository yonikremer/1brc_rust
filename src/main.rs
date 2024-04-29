use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::{fs, io};
use std::io::{BufRead, BufReader};
use std::num::ParseIntError;

const FILE_PATH: &str = "data/measurements.txt";

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

    // fn merge(&mut self, other: Self) -> <CityInfo as IntoFuture>::Output {
    //     self.max_temp = max(self.max_temp, other.max_temp);
    //     self.min_temp = min(self.min_temp, other.min_temp);
    //     self.num_measurements += other.num_measurements;
    //     self.sum_measurements += other.sum_measurements;
    // }
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


fn main() {
    let file = fs::File::open(FILE_PATH).expect("Please fix file name");
    let reader = BufReader::new(file);
    let mut map: HashMap<String, CityInfo> = HashMap::new();
    for result_line in reader.lines() {
        process_line(&mut map, result_line);
    }
    let raw_str = r#"{"#;
    print!("{raw_str}");
    for (city_name, city_info) in map.iter(){
        print!("{city_name}={city_info}, ");
    }
    let raw_str2 = r#"}"#;
    println!("{raw_str2}");
}

fn process_line(map: &mut HashMap<String, CityInfo>, result_line: io::Result<String>) {
    let line = result_line.unwrap();
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

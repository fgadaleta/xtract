use regex::Regex;
use lazy_static::lazy_static;

/// Checks whether all characters in this address are valid. Returns a true if all characters are
/// valid, false otherwise.
/// From https://docs.rs/iban_validate/0.3.1/src/iban/iban_standard.rs.html
fn validate_characters(address: &str) -> bool {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"^[A-Z]{2}\d{2}[A-Z\d]{1,30}$")
            .expect("Could not compile regular expression. Please file an issue at \
                https://github.com/ThomasdenH/iban_validate.");
    }
    RE.is_match(address)
}

pub fn validate_iban(address: &str) -> bool {
    return
    // Check the characters
    validate_characters(&address)
      // Check the checksum
      && compute_checksum(&address) == 1;
}

fn compute_checksum(address: &str) -> u8 {
    address.chars()
    // Move the first four characters to the back
    .cycle()
    .skip(4)
    .take(address.len())
    // Calculate the checksum
    .fold(0, |acc, c| {
      // Convert '0'-'Z' to 0-35
      let digit = c.to_digit(36)
        .expect("An address was supplied to compute_checksum with an invalid character. \
                    Please file an issue at https://github.com/ThomasdenH/iban_validate.");
      // If the number consists of two digits, multiply by 100
      let multiplier = if digit > 9 { 100 } else { 10 };
      // Calculate modulo
      (acc * multiplier + digit) % 97
    }) as u8
}

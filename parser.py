import re
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
import logging
import json

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Enhanced fuel mappings with better pattern matching
DEFAULT_FUEL_MAPPINGS = {
    # Diesel variations - with regex patterns for partial matching
    "Diesel": "Diesel",
    "DIESEL": "Diesel",
    "diesel": "Diesel",
    "DO": "Diesel",
    "do": "Diesel",
    "ប្រេងម៉ាស៊ូត": "Diesel",
    "Diesel Oil": "Diesel",
    "ម៉ាស៊ូត": "Diesel",
    "សាំងម៉ាស៊ូត": "Diesel",

    # Regular/EA92 variations
    "Regular": "Regular",
    "REGULAR": "សាំង EA92 - T2",
    "regular": "Regular",
    "EA92": "Regular",
    "ea92": "Regular",
    "សាំង": "Regular",
    "Gasoline": "Regular",
    "Petrol": "Regular",
    "Unleaded": "Regular",
    "ធម្មតា": "Regular",
    "សាំងធម្មតា": "សាំង EA92 - T2",
    
    # Super/EA95 variations
    "Super": "Super",
    "SUPER": "Super",
    "super": "Super",
    "EA95": "Super",
    "ea95": "Super",
    "សាំងស៊ុបពែរ": "Super",
    "Premium": "Super",
    "premium": "Super",
    "Super Premium": "Super",
    "ស៊ុបពែរ": "Super",
    "សាំងប្រេមីយ៉ូម": "Super",
}

class ReportParser:
    """Enhanced parser for fuel station daily reports."""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.fuel_mappings = self.config.get('fuel_mappings', DEFAULT_FUEL_MAPPINGS)
        self.station_prefixes = self.config.get('station_prefixes', [
            "ស្ថានីយ៍", "Station", "ស្ថានីយ", "ប៉ាន់", "អគ្គិសនី", "អ្នកលក់", "ហាង"
        ])
        
        # Compile regex patterns for better performance
        self.date_patterns = [
            (re.compile(r'(\d{1,2}-[A-Za-z]{3,9}-\d{4})'), "%d-%b-%Y"),
            (re.compile(r'(\d{1,2}/\d{1,2}/\d{4})'), "%d/%m/%Y"),
            (re.compile(r'(\d{1,2}/\d{1,2}/\d{2})'), "%d/%m/%y"),
            (re.compile(r'(\d{4}-\d{2}-\d{2})'), "%Y-%m-%d"),
            (re.compile(r'(\d{2}-\d{2}-\d{4})'), "%d-%m-%Y"),
            (re.compile(r'(\d{1,2}\.\d{1,2}\.\d{4})'), "%d.%m.%Y"),
        ]
        
        self.pump_pattern = re.compile(r'(Pump\s+\d+|P\d+|Pump\d+)', re.IGNORECASE)
        self.number_pattern = re.compile(r'[\d,]+\.?\d*')
        self.currency_pattern = re.compile(r'[\$\៛]')
        
    def parse_daily_report(self, report_text: str) -> Dict[str, Any]:
        """
        Parse daily fuel station report text into structured data.
        
        Expected format example:
        ស្ថានីយ៍ BVM ព្រែកអញ្ចាញ
        Summary Report
        27-Dec-2025 12:00 AM to 27-Dec-2025 11:59 PM
        Product	Volume	Amount
        Pump 1
        Diesel	309.82	272.65
        Regular	277.19	260.56
        Super	166.13	179.41
        ...
        Summary
        Product	Volume	Amount
        Diesel	790.26	695.45
        Regular	1798.43	1690.48
        Super	543.15	586.53
        Total Sale	3131.84	2972.46
        """
        
        try:
            logger.info("Parsing daily report...")
            
            # Initialize result structure
            result = {
                "station_name": "",
                "report_date": "",
                "pump_data": [],
                "summary_data": [],
                "fuel_data": [],
                "total_sales": {
                    "volume": 0.0,
                    "amount": 0.0
                },
                "metadata": {
                    "parsed_at": datetime.now().isoformat(),
                    "lines_processed": 0,
                    "has_summary": False,
                    "has_pump_data": False
                }
            }
            
            # Split lines and clean them
            lines = [line.strip() for line in report_text.split('\n') if line.strip()]
            
            if not lines:
                logger.error("Empty report text")
                return self._create_error_result("Empty report text")
            
            result["metadata"]["lines_processed"] = len(lines)
            logger.info(f"Processing {len(lines)} lines")
            
            # Step 1: Extract station name (first line usually contains station info)
            station_name = self.extract_station_name(lines[0])
            result["station_name"] = station_name
            logger.info(f"Station name: {station_name}")
            
            # Step 2: Find and extract report date
            report_date = self.extract_report_date(lines)
            result["report_date"] = report_date
            logger.info(f"Report date: {report_date}")
            
            # Step 3: Look for summary section first (most reliable)
            summary_data = self.parse_summary_data(lines)
            if summary_data:
                result["summary_data"] = summary_data
                result["metadata"]["has_summary"] = True
                logger.info(f"Found summary data: {len(summary_data)} items")
            
            # Step 4: Parse pump-by-pump data
            pumps_data = self.parse_pump_data(lines)
            result["pump_data"] = pumps_data
            if pumps_data:
                result["metadata"]["has_pump_data"] = True
                logger.info(f"Found {len(pumps_data)} pumps")
            
            # Step 5: Parse fuel data - prefer summary data if available
            if summary_data:
                fuel_data = summary_data
                logger.info("Using fuel data from summary section")
            elif pumps_data:
                fuel_data = self.extract_fuel_data_from_pumps(pumps_data)
                logger.info("Using fuel data aggregated from pumps")
            else:
                # Try to extract fuel data directly from lines
                fuel_data = self.extract_fuel_data_from_lines(lines)
                logger.info("Using fuel data extracted directly from lines")
            
            result["fuel_data"] = fuel_data
            
            # Step 6: Parse total sales
            total_sales = self.extract_total_sales(lines)
            result["total_sales"] = total_sales
            
            # Step 7: Map fuel types to the expected format in your database
            if fuel_data:
                mapped_fuel_data = self.map_fuel_types_to_standard(fuel_data)
                result["fuel_data"] = mapped_fuel_data
                logger.info(f"Mapped {len(mapped_fuel_data)} fuel types")
            
            # Step 8: Calculate totals if not extracted or inconsistent
            if result["total_sales"]["volume"] == 0.0 and result["fuel_data"]:
                totals = self.calculate_totals_from_fuel_data(result["fuel_data"])
                result["total_sales"] = totals
                logger.info(f"Calculated totals: {totals['volume']}L, ${totals['amount']:.2f}")
            elif result["total_sales"]["volume"] > 0 and result["fuel_data"]:
                # Verify totals consistency
                self.verify_totals_consistency(result)
            
            # Step 9: Validate the parsed data
            validation_result = self.validate_parsed_data(result)
            result["metadata"]["validation"] = validation_result
            
            if not validation_result["is_valid"]:
                logger.warning(f"Parsed data validation failed: {validation_result['errors']}")
            
            logger.info(f"Successfully parsed report for {station_name} on {report_date}")
            logger.info(f"Total volume: {result['total_sales']['volume']:.2f}L, Total amount: ${result['total_sales']['amount']:.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing report: {str(e)}", exc_info=True)
            return self._create_error_result(str(e))
    
    def _create_error_result(self, error_message: str) -> Dict[str, Any]:
        """Create a standardized error result."""
        return {
            "station_name": "UNKNOWN",
            "report_date": datetime.now().strftime("%d/%m/%y"),
            "pump_data": [],
            "summary_data": [],
            "fuel_data": [],
            "total_sales": {"volume": 0.0, "amount": 0.0},
            "metadata": {
                "parsed_at": datetime.now().isoformat(),
                "error": error_message,
                "is_valid": False
            }
        }
    
    def extract_station_name(self, first_line: str) -> str:
        """Extract station name from the first line of the report."""
        station_line = first_line.strip()
        logger.debug(f"Raw station line: '{station_line}'")
        
        # Remove common prefixes to get clean station name
        for prefix in self.station_prefixes:
            if station_line.startswith(prefix):
                station_line = station_line[len(prefix):].strip()
                logger.debug(f"Removed prefix '{prefix}', remaining: '{station_line}'")
                break
        
        # Also check if prefix is in the middle of the string
        for prefix in self.station_prefixes:
            if prefix in station_line and station_line != prefix:
                # Replace the prefix with empty string
                station_line = station_line.replace(prefix, '').strip()
                logger.debug(f"Removed prefix '{prefix}', remaining: '{station_line}'")
        
        # Remove multiple spaces
        station_line = re.sub(r'\s+', ' ', station_line)
        
        # Trim whitespace
        station_line = station_line.strip()
        
        # Clean up any trailing punctuation
        station_line = re.sub(r'^[,\-–—\.:;]+|[,\-–—\.:;]+$', '', station_line)
        
        # If empty after cleaning, use the original (with prefixes)
        if not station_line or station_line.isspace():
            logger.warning(f"Empty station name after cleaning, using original: '{first_line}'")
            # Return original but cleaned up
            return re.sub(r'\s+', ' ', first_line.strip())
        
        logger.debug(f"Cleaned station name: '{station_line}'")
        return station_line
    
    def extract_report_date(self, lines: List[str]) -> str:
        """
        Extract report date from lines with better pattern matching.
        Looks for date patterns like: 27-Dec-2025 12:00 AM to 27-Dec-2025 11:59 PM
        """
        # First, try to find a line with "Report" and date pattern
        for i, line in enumerate(lines):
            line_lower = line.lower()
            
            # Skip lines that are headers or summaries
            skip_keywords = ["product", "volume", "amount", "summary", "pump", 
                           "diesel", "regular", "super", "gasoline", "petrol"]
            if any(keyword in line_lower for keyword in skip_keywords):
                continue
            
            logger.debug(f"Checking line {i} for date: '{line}'")
            
            # Check for date range pattern first (e.g., "27-Dec-2025 12:00 AM to 27-Dec-2025 11:59 PM")
            date_range_pattern = re.compile(r'(\d{1,2}-[A-Za-z]{3,9}-\d{4}).*?to.*?(\d{1,2}-[A-Za-z]{3,9}-\d{4})', re.IGNORECASE)
            range_match = date_range_pattern.search(line)
            if range_match:
                date_str = range_match.group(1)  # Take the first date in the range
                try:
                    date_obj = self._parse_date_string(date_str)
                    formatted_date = date_obj.strftime("%d/%m/%y")
                    logger.info(f"Extracted date from range: {date_str} -> {formatted_date}")
                    return formatted_date
                except ValueError:
                    continue
            
            # Check for single date patterns
            for pattern, date_format in self.date_patterns:
                matches = pattern.findall(line)
                if matches:
                    # Take the first date found (usually the report date)
                    date_str = matches[0]
                    try:
                        date_obj = self._parse_date_string(date_str)
                        formatted_date = date_obj.strftime("%d/%m/%y")
                        logger.info(f"Extracted date: {date_str} -> {formatted_date}")
                        return formatted_date
                    except ValueError as e:
                        logger.warning(f"Failed to parse date '{date_str}' with format '{date_format}': {e}")
                        continue
        
        # If no date found, try to find any date-like string in the first 10 lines
        logger.warning("No standard date format found in early lines, searching entire document")
        
        for line in lines:
            # Look for any 3-number pattern that could be a date
            numbers = re.findall(r'\b\d{1,4}\b', line)
            if len(numbers) >= 3:
                try:
                    # Try different combinations for day, month, year
                    possible_dates = []
                    for combo in [(0, 1, 2), (1, 0, 2), (2, 1, 0)]:
                        day, month, year = map(int, [numbers[combo[0]], numbers[combo[1]], numbers[combo[2]]])
                        
                        # Validate reasonable date
                        if 1 <= day <= 31 and 1 <= month <= 12 and year > 100:
                            if year < 1000:  # 3-digit year (unlikely but handle it)
                                year += 1900
                            elif year < 100:  # 2-digit year
                                year += 2000 if year < 50 else 1900
                            
                            try:
                                date_obj = date(year, month, day)
                                possible_dates.append(date_obj)
                            except ValueError:
                                continue
                    
                    if possible_dates:
                        # Take the most recent date (likely the report date)
                        date_obj = max(possible_dates)
                        formatted_date = date_obj.strftime("%d/%m/%y")
                        logger.info(f"Extracted date from numbers: {formatted_date}")
                        return formatted_date
                        
                except (ValueError, IndexError) as e:
                    logger.debug(f"Failed to parse numbers as date: {e}")
                    continue
        
        # If still no date found, use today's date but log warning
        logger.warning("No date found in report, using today's date")
        today = datetime.now().strftime("%d/%m/%y")
        logger.info(f"Using today's date: {today}")
        return today
    
    def _parse_date_string(self, date_str: str) -> datetime:
        """Helper method to parse date string with flexible month handling."""
        # Normalize month names to abbreviations
        month_mapping = {
            'jan': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'apr': 'Apr',
            'may': 'May', 'jun': 'Jun', 'jul': 'Jul', 'aug': 'Aug',
            'sep': 'Sep', 'oct': 'Oct', 'nov': 'Nov', 'dec': 'Dec',
            'january': 'Jan', 'february': 'Feb', 'march': 'Mar',
            'april': 'Apr', 'june': 'Jun', 'july': 'Jul',
            'august': 'Aug', 'september': 'Sep', 'october': 'Oct',
            'november': 'Nov', 'december': 'Dec'
        }
        
        # Try to normalize the date string
        for month_lower, month_abbr in month_mapping.items():
            if month_lower in date_str.lower():
                # Replace full month name with abbreviation
                parts = re.split(r'[-./]', date_str)
                if len(parts) >= 2:
                    month_part = parts[1]
                    if month_part.lower() == month_lower:
                        parts[1] = month_abbr
                        date_str = '-'.join(parts)
                        break
        
        # Try different date formats
        date_formats = [
            "%d-%b-%Y", "%d-%b-%y", "%d/%m/%Y", "%d/%m/%y",
            "%Y-%m-%d", "%d.%m.%Y", "%d %b %Y", "%d %B %Y"
        ]
        
        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse date string: {date_str}")
    
    def parse_pump_data(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Parse pump-by-pump data from the report."""
        pumps = []
        current_pump = None
        
        # Find the start of pump data (look for "Pump 1" or similar)
        start_index = -1
        for i, line in enumerate(lines):
            if self.pump_pattern.search(line):
                start_index = i
                logger.debug(f"Found pump data start at line {i}: '{line}'")
                break
        
        if start_index == -1:
            logger.warning("No pump data found in report")
            return pumps
        
        # Parse from start_index until we hit "Summary" or end of file
        i = start_index
        while i < len(lines):
            line = lines[i].strip()
            
            # Check if we've reached the summary section
            if line.lower() == "summary" and i > start_index + 2:
                logger.debug(f"Reached summary section at line {i}")
                break
            
            # Check if this line indicates a new pump
            pump_match = self.pump_pattern.search(line)
            if pump_match:
                # Save previous pump if exists
                if current_pump and current_pump["fuels"]:
                    pumps.append(current_pump)
                    logger.debug(f"Added pump: {current_pump['pump_number']} with {len(current_pump['fuels'])} fuels")
                
                # Start new pump
                pump_number = pump_match.group(1)
                current_pump = {
                    "pump_number": pump_number,
                    "fuels": []
                }
                logger.debug(f"Started new pump: {pump_number}")
            elif current_pump:
                # Try to parse fuel data for current pump
                fuel_data = self.parse_fuel_line(line)
                if fuel_data:
                    current_pump["fuels"].append(fuel_data)
                    logger.debug(f"Added fuel to pump {current_pump['pump_number']}: {fuel_data['fuel_type']}")
                elif line and not any(keyword in line.lower() for keyword in ["product", "volume", "amount"]):
                    # If line is not empty and not a header, but couldn't be parsed as fuel data,
                    # it might indicate the end of current pump data
                    logger.debug(f"Non-fuel line in pump section: '{line}', might indicate pump end")
            
            i += 1
        
        # Add the last pump if it has data
        if current_pump and current_pump["fuels"]:
            pumps.append(current_pump)
            logger.debug(f"Added last pump: {current_pump['pump_number']}")
        
        logger.info(f"Parsed {len(pumps)} pumps with data")
        return pumps
    
    def parse_fuel_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a single line of fuel data with enhanced error handling."""
        # Skip empty lines or header lines
        if not line.strip():
            return None
        
        line_lower = line.lower()
        skip_keywords = ["product", "volume", "amount", "ស្តង់", "បរិមាណ", "តម្លៃ", 
                        "summary", "total", "pump", "សរុប"]
        if any(keyword in line_lower for keyword in skip_keywords):
            return None
        
        # Try multiple splitting methods
        parts = []
        
        # Method 1: Split by tabs (most common in formatted reports)
        if '\t' in line:
            parts = [p.strip() for p in line.split('\t') if p.strip()]
        
        # Method 2: Split by multiple spaces (2 or more)
        elif '  ' in line:
            parts = [p.strip() for p in re.split(r'\s{2,}', line) if p.strip()]
        
        # Method 3: Try regex pattern matching for lines like "Diesel 309.82 272.65"
        if not parts or len(parts) < 2:
            # Enhanced pattern that handles Khmer text and various separators
            pattern = r'([A-Za-z\u1780-\u17FF\s]+?)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)'
            match = re.search(pattern, line)
            if match:
                parts = [match.group(1).strip(), match.group(2), match.group(3)]
        
        # Method 4: Try pattern with optional amount
        if not parts or len(parts) < 2:
            pattern = r'([A-Za-z\u1780-\u17FF\s]+?)\s+([\d,]+\.?\d*)'
            match = re.search(pattern, line)
            if match:
                parts = [match.group(1).strip(), match.group(2)]
        
        # Method 5: Split by single spaces as last resort
        if not parts or len(parts) < 2:
            parts = line.split()
        
        if len(parts) >= 2:
            try:
                fuel_type = parts[0]
                
                # Clean numeric values (remove commas, currency symbols)
                def clean_number(num_str):
                    if not num_str:
                        return 0.0
                    # Remove commas, currency symbols, and non-numeric characters except decimal point
                    cleaned = re.sub(r'[^\d\.]', '', str(num_str))
                    return float(cleaned) if cleaned else 0.0
                
                if len(parts) >= 3:
                    volume = clean_number(parts[1])
                    amount = clean_number(parts[2])
                else:
                    volume = clean_number(parts[1])
                    amount = 0.0
                
                # Additional validation
                if volume <= 0:
                    logger.debug(f"Invalid volume ({volume}) for fuel: {fuel_type}")
                    return None
                
                return {
                    "fuel_type": fuel_type.strip(),
                    "volume": volume,
                    "amount": amount,
                    "unit_price": amount / volume if volume > 0 else 0.0
                }
                
            except (ValueError, IndexError, ZeroDivisionError) as e:
                logger.debug(f"Failed to parse fuel line '{line}': {e}")
                return None
        
        logger.debug(f"Could not parse fuel line (insufficient parts): '{line}'")
        return None
    
    def extract_fuel_data_from_lines(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Extract fuel data directly from lines when no pump/summary sections are found."""
        fuel_data = []
        fuel_totals = {}
        
        for line in lines:
            fuel_item = self.parse_fuel_line(line)
            if fuel_item:
                fuel_type = fuel_item["fuel_type"]
                if fuel_type not in fuel_totals:
                    fuel_totals[fuel_type] = {"volume": 0.0, "amount": 0.0}
                fuel_totals[fuel_type]["volume"] += fuel_item["volume"]
                fuel_totals[fuel_type]["amount"] += fuel_item["amount"]
        
        for fuel_type, totals in fuel_totals.items():
            fuel_data.append({
                "fuel_type": fuel_type,
                "volume": totals["volume"],
                "amount": totals["amount"]
            })
        
        logger.info(f"Extracted {len(fuel_data)} fuel types directly from lines")
        return fuel_data
    
    def parse_summary_data(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Parse summary section of the report."""
        summary = []
        
        # Find the summary section
        summary_start = -1
        for i, line in enumerate(lines):
            if line.strip().lower() in ["summary", "សរុប", "សេចក្តីសង្ខេប"]:
                summary_start = i
                logger.debug(f"Found summary at line {i}")
                break
        
        if summary_start == -1:
            logger.debug("No summary section found")
            return summary
        
        # Look for the header line after "Summary"
        i = summary_start + 1
        header_line = -1
        
        header_keywords = ["product", "volume", "amount", "ស្តង់", "បរិមាណ", "តម្លៃ"]
        
        while i < min(summary_start + 10, len(lines)):
            line_lower = lines[i].lower()
            # Check if this line contains header keywords
            if any(keyword in line_lower for keyword in header_keywords):
                header_line = i
                logger.debug(f"Found summary header at line {i}: '{lines[i]}'")
                break
            i += 1
        
        if header_line == -1:
            logger.warning("Could not find summary header")
            # Try to parse from summary_start + 1 anyway
            header_line = summary_start
        
        # Start parsing after header
        i = header_line + 1
        
        # Parse summary lines until we hit "Total Sale" or end or another section
        while i < len(lines):
            line = lines[i].strip()
            
            if not line:
                i += 1
                continue
            
            # Stop if we reach "Total Sale" or another section
            line_lower = line.lower()
            stop_keywords = ["total sale", "total sales", "total", "សរុប", "pump", "ស្តង់"]
            if any(keyword in line_lower for keyword in stop_keywords) and i > header_line + 1:
                logger.debug(f"Stopping summary parsing at line {i}: '{line}'")
                break
            
            # Stop if we've parsed too many lines without finding valid data
            if i - summary_start > 30:
                logger.debug("Stopping summary parsing: too many lines without valid data")
                break
            
            # Parse the summary line
            fuel_data = self.parse_fuel_line(line)
            if fuel_data:
                summary.append(fuel_data)
                logger.debug(f"Parsed summary item: {fuel_data['fuel_type']} = {fuel_data['volume']}L")
            else:
                # If we can't parse, might be end of summary
                logger.debug(f"Could not parse line as fuel data, stopping summary: '{line}'")
                break
            
            i += 1
        
        logger.info(f"Parsed {len(summary)} summary items")
        return summary
    
    def extract_fuel_data_from_pumps(self, pumps_data: List) -> List[Dict[str, Any]]:
        """Extract fuel data by aggregating pump data."""
        if not pumps_data:
            return []
        
        fuel_totals = {}
        for pump in pumps_data:
            for fuel in pump.get("fuels", []):
                fuel_type = fuel["fuel_type"]
                if fuel_type not in fuel_totals:
                    fuel_totals[fuel_type] = {"volume": 0.0, "amount": 0.0, "count": 0}
                fuel_totals[fuel_type]["volume"] += fuel["volume"]
                fuel_totals[fuel_type]["amount"] += fuel["amount"]
                fuel_totals[fuel_type]["count"] += 1
        
        fuel_data = []
        for fuel_type, totals in fuel_totals.items():
            fuel_data.append({
                "fuel_type": fuel_type,
                "volume": totals["volume"],
                "amount": totals["amount"],
                "pump_count": totals["count"]
            })
        
        logger.info(f"Aggregated {len(fuel_data)} fuel types from pump data")
        return fuel_data
    
    def extract_total_sales(self, lines: List[str]) -> Dict[str, float]:
        """Extract total sales from the report."""
        total_sales = {"volume": 0.0, "amount": 0.0}
        
        # Look for "Total Sale" line with various spellings
        total_keywords = ["total sale", "total sales", "total", "សរុប", "grand total"]
        
        for line in lines:
            line_lower = line.lower()
            
            for keyword in total_keywords:
                if keyword in line_lower:
                    logger.debug(f"Found total sales line: '{line}'")
                    
                    # Try to extract numbers using regex
                    numbers = self.number_pattern.findall(line)
                    if len(numbers) >= 2:
                        try:
                            total_sales["volume"] = float(numbers[0].replace(',', ''))
                            total_sales["amount"] = float(numbers[1].replace(',', ''))
                            logger.info(f"Extracted total sales: {total_sales['volume']:.2f}L, ${total_sales['amount']:.2f}")
                            return total_sales
                        except ValueError as e:
                            logger.warning(f"Failed to parse numbers from total sales line: {e}")
                    else:
                        # Try to parse tab-separated or space-separated format
                        parts = re.split(r'\t+|\s{2,}', line.strip())
                        if len(parts) >= 3:
                            try:
                                # Skip the first part (label) and parse next two as numbers
                                total_sales["volume"] = float(parts[1].replace(',', ''))
                                total_sales["amount"] = float(self.currency_pattern.sub('', parts[2]).replace(',', ''))
                                logger.info(f"Extracted total sales from parts: {total_sales['volume']:.2f}L, ${total_sales['amount']:.2f}")
                                return total_sales
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Failed to parse total sales from parts: {e}")
                    
                    # If we found a total line but couldn't parse it, break the inner loop
                    break
        
        logger.debug("No total sales line found or could not parse it")
        return total_sales
    
    def verify_totals_consistency(self, result: Dict[str, Any]) -> None:
        """Verify that calculated totals match extracted totals."""
        if not result["fuel_data"]:
            return
        
        calculated_total = self.calculate_totals_from_fuel_data(result["fuel_data"])
        extracted_total = result["total_sales"]
        
        if extracted_total["volume"] > 0 and calculated_total["volume"] > 0:
            volume_diff = abs(calculated_total["volume"] - extracted_total["volume"])
            volume_diff_pct = (volume_diff / extracted_total["volume"]) * 100
            
            amount_diff = abs(calculated_total["amount"] - extracted_total["amount"])
            amount_diff_pct = (amount_diff / extracted_total["amount"]) * 100 if extracted_total["amount"] > 0 else 100
            
            tolerance = 1.0  # 1% tolerance
            
            if volume_diff_pct > tolerance or amount_diff_pct > tolerance:
                logger.warning(
                    f"Totals mismatch: "
                    f"Volume: calculated={calculated_total['volume']:.2f}, "
                    f"extracted={extracted_total['volume']:.2f} ({volume_diff_pct:.2f}% diff). "
                    f"Amount: calculated={calculated_total['amount']:.2f}, "
                    f"extracted={extracted_total['amount']:.2f} ({amount_diff_pct:.2f}% diff)"
                )
                
                # Update with calculated totals if mismatch is significant
                if volume_diff_pct > 5 or amount_diff_pct > 5:
                    logger.info("Using calculated totals due to significant mismatch")
                    result["total_sales"] = calculated_total
    
    def map_fuel_types_to_standard(self, fuel_data: List[Dict]) -> List[Dict]:
        """
        Map the fuel types from the report to standard types used in the database.
        
        Enhanced mapping for better fuel type recognition.
        """
        mapped_data = []
        
        for item in fuel_data:
            fuel_type = item["fuel_type"].strip()
            original_fuel_type = fuel_type
            
            # Try exact match first
            mapped_type = self.fuel_mappings.get(fuel_type)
            
            # Try case-insensitive match
            if not mapped_type:
                mapped_type = self.fuel_mappings.get(fuel_type.lower())
            
            # Try to match by contains with priority to longer matches
            if not mapped_type:
                best_match = None
                best_match_length = 0
                
                for key, value in self.fuel_mappings.items():
                    key_lower = key.lower()
                    fuel_type_lower = fuel_type.lower()
                    
                    # Check if the key is contained in the fuel type or vice versa
                    if key_lower in fuel_type_lower or fuel_type_lower in key_lower:
                        # Prefer longer matches (more specific)
                        match_length = len(key) if key_lower in fuel_type_lower else len(fuel_type)
                        if match_length > best_match_length:
                            best_match = value
                            best_match_length = match_length
                            logger.debug(f"Partial match: '{fuel_type}' -> '{key}' (length: {match_length})")
                
                if best_match:
                    mapped_type = best_match
            
            # If still no match, try to infer from common patterns
            if not mapped_type:
                fuel_type_lower = fuel_type.lower()
                if any(word in fuel_type_lower for word in ["diesel", "do", "ម៉ាស៊ូត"]):
                    mapped_type = "ប្រេងម៉ាស៊ូត DO - T1"
                elif any(word in fuel_type_lower for word in ["regular", "ea92", "សាំង", "gasoline"]):
                    mapped_type = "សាំង EA92 - T2"
                elif any(word in fuel_type_lower for word in ["super", "ea95", "premium", "ស៊ុប"]):
                    mapped_type = "សាំងស៊ុបពែរ EA95 -T3"
                else:
                    # Keep the original but clean it up
                    clean_fuel_type = re.sub(
                        r'\(.*?\)|\[.*?\]|\s*L\s*|\s*litre\s*|\s*liters\s*|\s*ℓ\s*|\s*gal\s*|\s*gallon\s*',
                        '', fuel_type
                    )
                    clean_fuel_type = clean_fuel_type.strip()
                    mapped_type = clean_fuel_type
                    logger.debug(f"No mapping found for '{fuel_type}', using '{clean_fuel_type}'")
            
            # Create mapped item with all original data plus mapping info
            mapped_item = {
                "fuel_type": mapped_type,
                "original_fuel_type": original_fuel_type,
                "volume": item["volume"],
                "amount": item["amount"]
            }
            
            # Copy any additional fields
            for key, value in item.items():
                if key not in mapped_item:
                    mapped_item[key] = value
            
            mapped_data.append(mapped_item)
            
            if original_fuel_type != mapped_type:
                logger.debug(f"Mapped fuel type: '{original_fuel_type}' -> '{mapped_type}'")
        
        return mapped_data
    
    def calculate_totals_from_fuel_data(self, fuel_data: List[Dict]) -> Dict[str, float]:
        """Calculate total volume and amount from fuel data."""
        total_volume = 0.0
        total_amount = 0.0
        
        for item in fuel_data:
            total_volume += item.get("volume", 0.0)
            total_amount += item.get("amount", 0.0)
        
        logger.debug(f"Calculated totals: volume={total_volume:.2f}, amount={total_amount:.2f}")
        return {"volume": total_volume, "amount": total_amount}
    
    def validate_parsed_data(self, parsed_data: Dict) -> Dict[str, Any]:
        """Validate the parsed data for consistency and return detailed validation results."""
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "checks_passed": 0,
            "checks_total": 0
        }
        
        try:
            # Check 1: Required fields
            required_fields = ["station_name", "report_date", "fuel_data"]
            validation_result["checks_total"] += len(required_fields)
            
            for field in required_fields:
                if field not in parsed_data:
                    validation_result["errors"].append(f"Missing required field: {field}")
                    validation_result["is_valid"] = False
                else:
                    validation_result["checks_passed"] += 1
            
            # Check 2: Station name
            validation_result["checks_total"] += 1
            if not parsed_data.get("station_name") or parsed_data["station_name"] == "UNKNOWN":
                validation_result["warnings"].append("Station name is missing or default")
            else:
                validation_result["checks_passed"] += 1
            
            # Check 3: Report date format
            validation_result["checks_total"] += 1
            try:
                datetime.strptime(parsed_data.get("report_date", ""), "%d/%m/%y")
                validation_result["checks_passed"] += 1
            except ValueError:
                validation_result["warnings"].append(f"Invalid date format: {parsed_data.get('report_date')}")
            
            # Check 4: Fuel data
            validation_result["checks_total"] += 1
            if not parsed_data.get("fuel_data"):
                validation_result["warnings"].append("No fuel data parsed")
            else:
                validation_result["checks_passed"] += 1
                
                # Check for negative values
                validation_result["checks_total"] += 1
                has_negative = False
                for fuel in parsed_data["fuel_data"]:
                    if fuel.get("volume", 0) < 0 or fuel.get("amount", 0) < 0:
                        has_negative = True
                        break
                
                if not has_negative:
                    validation_result["checks_passed"] += 1
                else:
                    validation_result["warnings"].append("Negative values found in fuel data")
            
            # Check 5: Totals consistency
            validation_result["checks_total"] += 1
            if parsed_data.get("total_sales", {}).get("volume", 0) > 0 and parsed_data.get("fuel_data"):
                fuel_total = sum(item.get("volume", 0) for item in parsed_data["fuel_data"])
                if fuel_total > 0:
                    diff_pct = abs(fuel_total - parsed_data["total_sales"]["volume"]) / parsed_data["total_sales"]["volume"] * 100
                    
                    if diff_pct <= 5:  # Within 5% tolerance
                        validation_result["checks_passed"] += 1
                    else:
                        validation_result["warnings"].append(
                            f"Volume mismatch: fuel total={fuel_total:.2f}, "
                            f"reported={parsed_data['total_sales']['volume']:.2f} ({diff_pct:.1f}% diff)"
                        )
                else:
                    validation_result["checks_passed"] += 1
            else:
                validation_result["checks_passed"] += 1
            
            # Calculate validation score
            if validation_result["checks_total"] > 0:
                validation_result["score"] = (validation_result["checks_passed"] / validation_result["checks_total"]) * 100
            else:
                validation_result["score"] = 0
            
            logger.info(f"Validation: {validation_result['checks_passed']}/{validation_result['checks_total']} checks passed "
                       f"({validation_result['score']:.1f}%)")
            
            return validation_result
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
            return validation_result
    
    def format_for_database(self, parsed_data: Dict) -> Dict:
        """
        Format parsed data for database storage.
        This is the main function that should be called by your bot.
        """
        try:
            # Calculate totals if not already calculated
            if parsed_data.get("total_sales", {}).get("volume", 0) == 0.0 and parsed_data.get("fuel_data"):
                totals = self.calculate_totals_from_fuel_data(parsed_data["fuel_data"])
                parsed_data["total_sales"] = totals
            
            # Get validation result
            validation_result = parsed_data.get("metadata", {}).get("validation", {})
            if not validation_result:
                validation_result = self.validate_parsed_data(parsed_data)
            
            # Create database-friendly structure
            db_data = {
                "station_name": parsed_data.get("station_name", "UNKNOWN"),
                "report_date": parsed_data.get("report_date", datetime.now().strftime("%d/%m/%y")),
                "fuel_data": parsed_data.get("fuel_data", []),
                "total_volume": parsed_data.get("total_sales", {}).get("volume", 0.0),
                "total_amount": parsed_data.get("total_sales", {}).get("amount", 0.0),
                "pump_count": len(parsed_data.get("pump_data", [])),
                "parsed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "validation_score": validation_result.get("score", 0),
                "is_valid": validation_result.get("is_valid", False),
                "validation_warnings": validation_result.get("warnings", []),
                "metadata": parsed_data.get("metadata", {})
            }
            
            # Add summary statistics
            if parsed_data.get("fuel_data"):
                db_data["fuel_types_count"] = len(parsed_data["fuel_data"])
                db_data["average_unit_price"] = (
                    db_data["total_amount"] / db_data["total_volume"] 
                    if db_data["total_volume"] > 0 else 0.0
                )
            
            logger.info(f"Formatted for database: {db_data['station_name']} - {db_data['report_date']} "
                       f"(Validation: {db_data['validation_score']:.1f}%)")
            
            return db_data
            
        except Exception as e:
            logger.error(f"Error formatting for database: {e}")
            # Return basic structure with available data
            return {
                "station_name": parsed_data.get("station_name", "UNKNOWN"),
                "report_date": parsed_data.get("report_date", datetime.now().strftime("%d/%m/%y")),
                "fuel_data": parsed_data.get("fuel_data", []),
                "total_volume": parsed_data.get("total_sales", {}).get("volume", 0.0),
                "total_amount": parsed_data.get("total_sales", {}).get("amount", 0.0),
                "pump_count": len(parsed_data.get("pump_data", [])),
                "parsed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "is_valid": False,
                "error": str(e)
            }
    
    def export_to_json(self, parsed_data: Dict, filename: Optional[str] = None) -> str:
        """Export parsed data to JSON format."""
        try:
            # Format for database first
            db_data = self.format_for_database(parsed_data)
            
            # Convert to JSON with proper formatting
            json_data = json.dumps(db_data, indent=2, ensure_ascii=False, default=str)
            
            if filename:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(json_data)
                logger.info(f"Exported data to {filename}")
            
            return json_data
            
        except Exception as e:
            logger.error(f"Error exporting to JSON: {e}")
            return json.dumps({"error": str(e)}, indent=2)


# Backward compatibility functions
def parse_daily_report(report_text: str) -> Dict[str, Any]:
    """Backward compatibility wrapper for the parser."""
    parser = ReportParser()
    return parser.parse_daily_report(report_text)

def format_for_database(parsed_data: Dict) -> Dict:
    """Backward compatibility wrapper for database formatting."""
    parser = ReportParser()
    return parser.format_for_database(parsed_data)


def create_sample_reports() -> Dict[str, str]:
    """Create sample reports for testing."""
    samples = {}
    
    samples["standard_report"] = """ស្ថានីយ៍ BVM ព្រែកអញ្ចាញ		
			
	Summary Report		
	27-Dec-2025 12:00 AM to 27-Dec-2025 11:59 PM		
	Product	Volume	Amount
	Pump 1		
	Diesel	309.82	272.65
	Regular	277.19	260.56
	Super	166.13	179.41
	Pump 2		
	Diesel	35	30.8
	Regular	294.6	276.92
	Super	103.4	111.65
	Pump 3		
	Diesel	268.17	236
	Regular	531.06	499.19
	Super	70.43	76.05
	Pump 4		
	Diesel	40.91	36
	Regular	136.25	128.06
	Super	159.16	171.89
	Pump 5		
	Diesel	97.04	85.4
	Regular	363.62	341.78
	Super	38.58	41.65
	Pump 6		
	Diesel	20	17.6
	Regular	77.46	72.81
	Super	3.53	3.81
	Pump 7		
	Diesel	19.32	17
	Regular	110.93	104.28
	Super	1.92	2.07
	Pump 8		
	Regular	7.32	6.88
			
	Summary		
	Product	Volume	Amount
	Diesel	790.26	695.45
	Regular	1798.43	1690.48
	Super	543.15	586.53
	Total Sale	3131.84	2972.46"""
    
    samples["koh_norea_report"] = """BMV កោះនរា		
			
	Summary Report		
	26-Nov-2025 12:00 AM to 26-Nov-2025 11:59 PM		
	Product	Volume	Amount
	Pump 1		
	Diesel	229.75	202.18
	Regular	485.75	456.6
	Super	88.74	95.83
	Pump 2		
	Diesel	10	8.8
	Regular	682.13	641.17
	Super	115.56	124.8
	Pump 3		
	Diesel	197.89	174.14
	Regular	1016.14	955.15
	Super	74.02	79.93
	Pump 4		
	Diesel	71.36	62.8
	Regular	712.96	670.17
	Super	134.15	144.87
	Pump 5		
	Diesel	180.69	159.01
	Regular	1030.56	968.66
	Super	147.55	159.32
	Pump 6		
	Diesel	16.36	14.4
	Regular	348.41	327.5
	Super	270.48	292.09
			
	Summary		
	Product	Volume	Amount
	Diesel	706.05	621.33
	Regular	4275.95	4019.25
	Super	830.5	896.84
	Total Sale	5812.5	5537.42"""
    
    samples["khmer_only_report"] = """ស្ថានីយ៍ ភ្នំពេញ
    ២៨/១២/២០២៥
    ប្រេងម៉ាស៊ូត ១០០ ៩០
    សាំង ២០០ ១៨០
    សាំងស៊ុបពែរ ៥០ ៥៥
    សរុប ៣៥០ ៣២៥"""
    
    samples["minimal_report"] = """Station Test
    01/01/2025
    Diesel 100.5 90.45
    Regular 150.2 141.19
    Super 75.8 81.86
    Total 326.5 313.5"""
    
    return samples


def run_comprehensive_tests():
    """Run comprehensive tests on the improved parser."""
    print("=" * 70)
    print("IMPROVED FUEL STATION REPORT PARSER - COMPREHENSIVE TESTS")
    print("=" * 70)
    
    parser = ReportParser()
    samples = create_sample_reports()
    
    test_results = {}
    
    for test_name, report_text in samples.items():
        print(f"\n{'='*40}")
        print(f"TESTING: {test_name.upper()}")
        print(f"{'='*40}")
        
        try:
            result = parser.parse_daily_report(report_text)
            db_format = parser.format_for_database(result)
            
            test_results[test_name] = {
                "success": True,
                "station": result["station_name"],
                "date": result["report_date"],
                "fuel_types": len(result["fuel_data"]),
                "pumps": len(result["pump_data"]),
                "total_volume": result["total_sales"]["volume"],
                "validation_score": db_format.get("validation_score", 0)
            }
            
            print(f"✓ Parsed successfully")
            print(f"  Station: {result['station_name']}")
            print(f"  Date: {result['report_date']}")
            print(f"  Fuel types: {len(result['fuel_data'])}")
            print(f"  Pumps: {len(result['pump_data'])}")
            print(f"  Total volume: {result['total_sales']['volume']:.2f}L")
            print(f"  Total amount: ${result['total_sales']['amount']:.2f}")
            print(f"  Validation score: {db_format.get('validation_score', 0):.1f}%")
            
            # Show fuel data summary
            if result["fuel_data"]:
                print(f"\n  Fuel Summary:")
                for fuel in result["fuel_data"]:
                    fuel_type = fuel.get("original_fuel_type", fuel["fuel_type"])
                    mapped_type = fuel["fuel_type"]
                    if fuel_type != mapped_type:
                        print(f"    {fuel_type} → {mapped_type}: {fuel['volume']:.2f}L, ${fuel['amount']:.2f}")
                    else:
                        print(f"    {fuel_type}: {fuel['volume']:.2f}L, ${fuel['amount']:.2f}")
            
        except Exception as e:
            test_results[test_name] = {
                "success": False,
                "error": str(e)
            }
            print(f"✗ Failed: {e}")
    
    # Summary
    print(f"\n{'='*70}")
    print("TEST SUMMARY")
    print(f"{'='*70}")
    
    successful = sum(1 for r in test_results.values() if r.get("success", False))
    total = len(test_results)
    
    print(f"Total tests: {total}")
    print(f"Successful: {successful}")
    print(f"Failed: {total - successful}")
    
    if successful > 0:
        avg_validation_score = sum(
            r.get("validation_score", 0) for r in test_results.values() 
            if r.get("success", False)
        ) / successful
        print(f"Average validation score: {avg_validation_score:.1f}%")
    
    # Export one sample to JSON
    if test_results.get("koh_norea_report", {}).get("success", False):
        print(f"\n{'='*70}")
        print("JSON EXPORT DEMO (Koh Norea Report)")
        print(f"{'='*70}")
        
        result = parser.parse_daily_report(samples["koh_norea_report"])
        json_output = parser.export_to_json(result)
        
        # Print first 500 chars of JSON
        preview = json_output[:500] + "..." if len(json_output) > 500 else json_output
        print(preview)
    
    return test_results


if __name__ == "__main__":
    # Run comprehensive tests
    test_results = run_comprehensive_tests()
    
    # Optional: Test with custom report
    print(f"\n{'='*70}")
    print("CUSTOM REPORT TEST")
    print(f"{'='*70}")
    
    custom_report = input("\nPaste a custom report to test (or press Enter to skip): ").strip()
    
    if custom_report:
        parser = ReportParser()
        try:
            result = parser.parse_daily_report(custom_report)
            db_format = parser.format_for_database(result)
            
            print(f"\n✓ Parsed successfully")
            print(f"  Station: {result['station_name']}")
            print(f"  Date: {result['report_date']}")
            print(f"  Validation: {db_format.get('validation_score', 0):.1f}%")
            print(f"  Is valid: {db_format.get('is_valid', False)}")
            
            if db_format.get("validation_warnings"):
                print(f"\n  Warnings:")
                for warning in db_format["validation_warnings"]:
                    print(f"    - {warning}")
            
        except Exception as e:
            print(f"\n✗ Failed to parse: {e}")
    
    print(f"\n{'='*70}")
    print("ALL TESTS COMPLETED")
    print(f"{'='*70}")
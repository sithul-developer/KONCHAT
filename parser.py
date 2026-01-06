import re
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
import logging
import json
from difflib import SequenceMatcher

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Enhanced fuel mappings with better pattern matching
DEFAULT_FUEL_MAPPINGS = {
    # Diesel variations
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
    "REGULAR": "Regular",
    "regular": "Regular",
    "EA92": "Regular",
    "ea92": "Regular",
    "សាំង": "Regular",
    "Gasoline": "Regular",
    "Petrol": "Regular",
    "Unleaded": "Regular",
    "92": "Regular",
    "អ៊ីអេ ៩២": "Regular",

    # Super/EA95 variations
    "Super": "Super",
    "SUPER": "Super",
    "super": "Super",
    "Premium": "Super",
    "premium": "Super",
    "EA95": "Super",
    "ea95": "Super",
    "95": "Super",
    "អ៊ីអេ ៩៥": "Super",
}


class ReportParser:
    """Enhanced parser for fuel station daily reports with specific format handling."""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.fuel_mappings = self.config.get('fuel_mappings', DEFAULT_FUEL_MAPPINGS)
        
        # KNOWN STATION LOCATIONS SPECIFIC TO YOUR SYSTEM
        # Based on "BVM [Location]" format
        self.known_station_locations = self.config.get('known_station_locations', [
            # Format: Khmer name (as appears in reports)
            "ព្រែកអញ្ចាញ",      # Prek Anchang
            "កោះនរា",          # Koh Norea
            "ផ្សារចាស់",        # Psar Chas (Old Market)
            "ភ្នំពេញ",          # Phnom Penh (central)
            "ទួលគោក",          # Tuol Kork
            "ឫស្សីកែវ",        # Russey Keo
            "សែនសុខ",          # Sen Sok
            "ពោធិ៍សែនជ័យ",    # Por Sen Chey
            "មានជ័យ",          # Mean Chey
            "ចំការដូង",        # Chomka Daun
            "បឹងកេងកង",       # Boeng Keng Kang (BKK)
            "ជ្រោយចង្វារ",      # Chroy Changvar
        ])
        
        # Company name variations
        self.company_names = ["BVM", "BMV", "ភីវីអេម", "Total", "Caltex", "PTT", "ស្ថានីយ៍"]
        
        # Expected station name patterns for your system
        # Primary pattern: "BVM" followed by Khmer location name
        self.station_pattern_primary = re.compile(
            r'^(?:BVM|BMV|ភីវីអេម)[\s\-]*([ខគឃងចឆជឈញដឋឌឍណតថទធនបផពភមយរលវឝឞសហឡអ\u17D2\u179F\u17D2\u1791\u17B6\u1793\u17B8\u1799៖\s]+.*?)$',
            re.IGNORECASE
        )
        
        # Secondary pattern: Khmer location name followed by "BVM"
        self.station_pattern_secondary = re.compile(
            r'^([ខគឃងចឆជឈញដឋឌឍណតថទធនបផពភមយរលវឝឞសហឡអ\u17D2\u179F\u17D2\u1791\u17B6\u1793\u17B8\u1799៖\s]+.*?)(?:\s*(?:BVM|BMV|ភីវីអេម))?$',
            re.IGNORECASE
        )
        
        # Fallback: Look for any Khmer text that looks like a location
        self.station_pattern_fallback = re.compile(
            r'([ខគឃងចឆជឈញដឋឌឍណតថទធនបផពភមយរលវឝឞសហឡអ]+(?:\s+[ខគឃងចឆជឈញដឋឌឍណតថទធនបផពភមយរលវឝឞសហឡអ]+)*)',
            re.IGNORECASE
        )
        
        # Compile regex patterns for better performance
        self.date_patterns = [
            (re.compile(r'(\d{1,2}-[A-Za-z]{3,9}-\d{4})'), "%d-%b-%Y"),
            (re.compile(r'(\d{1,2}/\d{1,2}/\d{4})'), "%d/%m/%Y"),
            (re.compile(r'(\d{1,2}/\d{1,2}/\d{2})'), "%d/%m/%y"),
            (re.compile(r'(\d{4}-\d{2}-\d{2})'), "%Y-%m-%d"),
            (re.compile(r'(\d{2}-\d{2}-\d{4})'), "%d-%m-%Y"),
            (re.compile(r'(\d{1,2}\.\d{1,2}\.\d{4})'), "%d.%m.%Y"),
        ]
        
        # Enhanced pump pattern
        self.pump_pattern = re.compile(r'(Pump\s*\d+|P\d+|Pump\d+|ស្តង់\s*\d+|P\s*\d+)', re.IGNORECASE)
        
        # Enhanced fuel line pattern (handles tabs, spaces, Khmer numbers)
        self.fuel_line_pattern = re.compile(
            r'([A-Za-z\u1780-\u17FF][A-Za-z\u1780-\u17FF\s]*?)\s+'
            r'([\d០១២៣៤៥៦៧៨៩,]+\.?\d*)\s+'
            r'([\d០១២៣៤៥៦៧៨៩,$៛]+\.?\d*)',
            re.IGNORECASE
        )
        
        # Summary section pattern
        self.summary_pattern = re.compile(r'(?:Summary|សរុប|សេចក្តីសង្ខេប)', re.IGNORECASE)
        
        # Total sale pattern
        self.total_sale_pattern = re.compile(
            r'(?:Total\s+Sale|Total\s+Sales|Grand\s+Total|សរុប|សរុបការលក់)'
            r'[:\s]*([\d០១២៣៤៥៦៧៨៩,]+\.?\d*)[\s,]+([\d០១២៣៤៥៦៧៨៩,$៛]+\.?\d*)',
            re.IGNORECASE
        )
        
        # Number pattern for extracting numbers
        self.number_pattern = re.compile(r'[\d០១២៣៤៥៦៧៨៩,]+\.?\d*')
        
    def parse_daily_report(self, report_text: str) -> Dict[str, Any]:
        """
        Parse daily fuel station report text into structured data.
        
        Expected format example:
        BVM ព្រែកអញ្ចាញ
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
            logger.info("=" * 60)
            logger.info("STARTING REPORT PARSING")
            logger.info("=" * 60)
            
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
                    "has_pump_data": False,
                    "parsing_method": "standard",
                    "station_format_detected": "unknown"
                }
            }
            
            # Step 1: Pre-process the text
            logger.info("Step 1: Pre-processing text...")
            processed_text = self.preprocess_report_text(report_text)
            lines = processed_text.split('\n')
            lines = [line.rstrip() for line in lines if line.strip()]
            
            if not lines:
                logger.error("Empty report text after preprocessing")
                return self._create_error_result("Empty report text")
            
            result["metadata"]["lines_processed"] = len(lines)
            logger.info(f"Processing {len(lines)} lines")
            
            # Log first few lines for debugging
            for i, line in enumerate(lines[:5]):
                logger.debug(f"Line {i}: '{line}'")
            
            # Step 2: Extract station name with enhanced method
            logger.info("Step 2: Extracting station name...")
            station_info = self.extract_station_name_for_bvm_system(lines)
            
            result["station_name"] = station_info["name"]
            result["metadata"]["station_format_detected"] = station_info["format"]
            result["metadata"]["station_confidence"] = station_info["confidence"]
            
            logger.info(f"✅ Station name: '{station_info['name']}'")
            logger.info(f"📊 Format: {station_info['format']}, Confidence: {station_info['confidence']:.2f}")
            
            # Step 3: Extract report date
            logger.info("Step 3: Extracting report date...")
            report_date = self.extract_report_date_enhanced(lines)
            result["report_date"] = report_date
            logger.info(f"✅ Report date: {report_date}")
            
            # Step 4: Parse fuel data
            logger.info("Step 4: Parsing fuel data...")
            
            # Try different parsing methods in order of reliability
            summary_data = self.parse_summary_section_enhanced(lines)
            if summary_data:
                result["summary_data"] = summary_data
                result["metadata"]["has_summary"] = True
                result["metadata"]["parsing_method"] = "summary_section"
                logger.info(f"✅ Found summary data: {len(summary_data)} items")
            
            pumps_data = self.parse_pump_data_enhanced(lines)
            if pumps_data:
                result["pump_data"] = pumps_data
                result["metadata"]["has_pump_data"] = True
                if not result["metadata"]["has_summary"]:
                    result["metadata"]["parsing_method"] = "pump_data"
                logger.info(f"✅ Found {len(pumps_data)} pumps with data")
            
            # Determine which fuel data to use
            if summary_data:
                fuel_data = summary_data
                logger.info("Using fuel data from summary section")
            elif pumps_data:
                fuel_data = self.aggregate_fuel_from_pumps(pumps_data)
                logger.info("Using fuel data aggregated from pumps")
            else:
                fuel_data = self.extract_fuel_data_direct(lines)
                result["metadata"]["parsing_method"] = "direct_extraction"
                logger.info(f"Extracted {len(fuel_data)} fuel types directly from lines")
            
            result["fuel_data"] = fuel_data
            
            # Step 5: Extract total sales
            logger.info("Step 5: Extracting total sales...")
            total_sales = self.extract_total_sales_enhanced(lines)
            result["total_sales"] = total_sales
            
            # Step 6: Map fuel types to standard format
            logger.info("Step 6: Mapping fuel types...")
            if fuel_data:
                mapped_fuel_data = self.map_fuel_types_enhanced(fuel_data)
                result["fuel_data"] = mapped_fuel_data
                logger.info(f"Mapped {len(mapped_fuel_data)} fuel types")
            
            # Step 7: Calculate and verify totals
            logger.info("Step 7: Verifying totals...")
            self.verify_and_calculate_totals(result)
            
            # Step 8: Validate parsed data
            logger.info("Step 8: Validating parsed data...")
            validation_result = self.validate_parsed_data_enhanced(result)
            result["metadata"]["validation"] = validation_result
            
            # Log parsing completion
            logger.info("=" * 60)
            logger.info(f"✅ PARSING COMPLETE: {station_info['name']} - {report_date}")
            logger.info(f"📊 Total volume: {result['total_sales']['volume']:.2f}L")
            logger.info(f"💰 Total amount: ${result['total_sales']['amount']:.2f}")
            logger.info(f"⛽ Fuel types: {len(result['fuel_data'])}")
            logger.info(f"📋 Parsing method: {result['metadata']['parsing_method']}")
            logger.info(f"🎯 Validation score: {validation_result.get('score', 0):.1f}%")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error parsing report: {str(e)}", exc_info=True)
            return self._create_error_result(str(e))
    
    def extract_station_name_for_bvm_system(self, lines: List[str]) -> Dict[str, Any]:
        """
        Extract station name specifically for BVM system format:
        Expected format: "BVM ព្រែកអញ្ចាញ" or similar
        """
        if not lines:
            return {
                "name": "Unknown Station",
                "format": "unknown",
                "confidence": 0.0
            }
        
        # Look through first 5 lines for station name
        for i, line in enumerate(lines[:5]):
            line = line.strip()
            if not line:
                continue
            
            logger.debug(f"Checking line {i} for station name: '{line}'")
            
            # STRATEGY 1: Direct match for "BVM [Khmer text]" pattern
            match = self.station_pattern_primary.search(line)
            if match:
                station_location = match.group(1).strip()
                if self._is_valid_khmer_location(station_location):
                    formatted_name = f"BVM {station_location}"
                    logger.info(f"✅ Found BVM format station: '{formatted_name}'")
                    return {
                        "name": formatted_name,
                        "format": "bvm_khmer",
                        "confidence": 0.95
                    }
            
            # STRATEGY 2: Look for known station locations
            for location in self.known_station_locations:
                if location in line:
                    # Check if BVM is in the line or nearby
                    has_bvm = "BVM" in line or "BMV" in line or "ភីវីអេម" in line
                    
                    if has_bvm:
                        formatted_name = f"BVM {location}"
                    else:
                        # If no BVM in line, check previous line
                        if i > 0 and any(company in lines[i-1] for company in ["BVM", "BMV", "ភីវីអេម"]):
                            formatted_name = f"BVM {location}"
                        else:
                            formatted_name = f"BVM {location}"  # Still use BVM prefix
                    
                    logger.info(f"✅ Found known location '{location}' in line")
                    return {
                        "name": formatted_name,
                        "format": "known_location",
                        "confidence": 0.90 if has_bvm else 0.80
                    }
            
            # STRATEGY 3: Look for any Khmer text that could be a location
            if self._contains_khmer_text(line):
                # Clean the line to extract potential location
                potential_location = self._extract_potential_location(line)
                if potential_location and len(potential_location) >= 3:
                    # Check if it looks like a valid location name
                    if self._looks_like_location_name(potential_location):
                        # Check if BVM is mentioned in this or previous line
                        has_bvm = any(company in line for company in ["BVM", "BMV", "ភីវីអេម"])
                        if i > 0:
                            has_bvm = has_bvm or any(company in lines[i-1] for company in ["BVM", "BMV", "ភីវីអេម"])
                        
                        formatted_name = f"BVM {potential_location}"
                        logger.info(f"✅ Extracted location from Khmer text: '{formatted_name}'")
                        return {
                            "name": formatted_name,
                            "format": "khmer_text_extracted",
                            "confidence": 0.85 if has_bvm else 0.75
                        }
        
        # STRATEGY 4: Fallback - look for any Khmer text in first 3 lines
        for i in range(min(3, len(lines))):
            line = lines[i].strip()
            if self._contains_khmer_text(line):
                # Extract longest Khmer word sequence
                khmer_words = re.findall(r'[\u1780-\u17FF]+', line)
                if khmer_words:
                    longest_word = max(khmer_words, key=len)
                    if len(longest_word) >= 4:  # Minimum length for location name
                        formatted_name = f"BVM {longest_word}"
                        logger.warning(f"⚠️ Using fallback Khmer text as station: '{formatted_name}'")
                        return {
                            "name": formatted_name,
                            "format": "fallback_khmer",
                            "confidence": 0.60
                        }
        
        # STRATEGY 5: Ultimate fallback - use first non-empty line
        for line in lines[:3]:
            if line.strip():
                cleaned = self._clean_line_for_station(line.strip())
                if cleaned and len(cleaned) >= 3:
                    formatted_name = f"BVM {cleaned}"
                    logger.warning(f"⚠️ Using first line fallback: '{formatted_name}'")
                    return {
                        "name": formatted_name,
                        "format": "first_line_fallback",
                        "confidence": 0.50
                    }
        
        return {
            "name": "Unknown Station",
            "format": "unknown",
            "confidence": 0.0
        }
    
    def _is_valid_khmer_location(self, text: str) -> bool:
        """Check if text is a valid Khmer location name."""
        if not text or len(text) < 3:
            return False
        
        # Should contain mostly Khmer characters
        khmer_chars = re.findall(r'[\u1780-\u17FF]', text)
        if len(khmer_chars) < len(text) * 0.6:  # At least 60% Khmer characters
            return False
        
        # Check against known invalid patterns
        invalid_patterns = [
            r'^សង្ខេប$',
            r'^របាយការណ៍$',
            r'^ប្រចាំថ្ងៃ$',
            r'^ស្ថានីយ៍$',
            r'^Summary$',
            r'^Report$',
            r'^Daily$',
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, text.strip(), re.IGNORECASE):
                return False
        
        return True
    
    def _contains_khmer_text(self, text: str) -> bool:
        """Check if text contains Khmer characters."""
        return bool(re.search(r'[\u1780-\u17FF]', text))
    
    def _extract_potential_location(self, text: str) -> str:
        """Extract potential location name from text."""
        # Remove common prefixes and suffixes
        text = re.sub(r'^(?:ស្ថានីយ៍|Station|BVM|BMV|ភីវីអេម)[\s\-:]*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'[\s\-]*(?:Summary|Report|របាយការណ៍|សេចក្តីសង្ខេប)[\s\-]*$', '', text, flags=re.IGNORECASE)
        
        # Remove date patterns
        text = re.sub(r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', '', text)
        text = re.sub(r'\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}', '', text)
        
        # Extract Khmer text
        khmer_parts = re.findall(r'[\u1780-\u17FF][\u1780-\u17FF\s]*[\u1780-\u17FF]', text)
        if khmer_parts:
            # Take the longest Khmer sequence
            return max(khmer_parts, key=len).strip()
        
        # Fallback: return cleaned text
        return text.strip()
    
    def _looks_like_location_name(self, text: str) -> bool:
        """Check if text looks like a location name."""
        if not text or len(text) < 3:
            return False
        
        # Location names typically don't contain numbers or special symbols
        if re.search(r'[\d@#$%^&*()]', text):
            return False
        
        # Should have at least one Khmer character
        if not self._contains_khmer_text(text):
            return False
        
        # Check for common location prefixes in Khmer
        location_prefixes = ["ព្រែក", "កោះ", "ផ្សារ", "ទួល", "ឫស្សី", "សែន", "ពោធិ៍", "មាន", "ចំការ", "បឹង", "ជ្រោយ"]
        for prefix in location_prefixes:
            if text.startswith(prefix):
                return True
        
        # Check if it's in known locations
        for location in self.known_station_locations:
            if location in text:
                return True
        
        return len(text) >= 4  # Minimum length for location name
    
    def _clean_line_for_station(self, line: str) -> str:
        """Clean a line to extract station information."""
        # Remove date patterns
        line = re.sub(r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', '', line)
        line = re.sub(r'\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}', '', line)
        
        # Remove report-related text
        report_terms = ["Summary Report", "Daily Report", "របាយការណ៍", "សេចក្តីសង្ខេប", "Report", "Summary"]
        for term in report_terms:
            line = line.replace(term, "")
        
        # Remove company names if at beginning
        for company in self.company_names:
            if line.startswith(company):
                line = line[len(company):].strip()
        
        # Clean up
        line = re.sub(r'\s+', ' ', line)
        line = line.strip(' -–—.:;|')
        
        return line.strip()
    
    def preprocess_report_text(self, text: str) -> str:
        """Pre-process the report text to handle various formats."""
        if not text:
            return ""
        
        # Replace tabs with spaces for consistent parsing
        text = text.replace('\t', '    ')
        
        # Handle multiple spaces
        text = re.sub(r' +', ' ', text)
        
        # Handle Khmer numbers (convert to Western if needed)
        khmer_to_western = {
            '០': '0', '១': '1', '២': '2', '៣': '3', '៤': '4',
            '៥': '5', '៦': '6', '៧': '7', '៨': '8', '៩': '9'
        }
        
        for khmer, western in khmer_to_western.items():
            text = text.replace(khmer, western)
        
        # Remove BOM character if present
        text = text.replace('\ufeff', '')
        
        return text
    
    def extract_report_date_enhanced(self, lines: List[str]) -> str:
        """Enhanced date extraction with multiple fallback strategies."""
        # Strategy 1: Look for date range pattern
        for line in lines:
            date_range_match = re.search(
                r'(\d{1,2}-[A-Za-z]{3,9}-\d{4}).*?to.*?(\d{1,2}-[A-Za-z]{3,9}-\d{4})',
                line,
                re.IGNORECASE
            )
            if date_range_match:
                date_str = date_range_match.group(1)  # Start date
                try:
                    date_obj = self._parse_date_string(date_str)
                    return date_obj.strftime("%d/%m/%y")
                except ValueError:
                    continue
        
        # Strategy 2: Look for single dates with various patterns
        for line in lines:
            for pattern, date_format in self.date_patterns:
                matches = pattern.findall(line)
                if matches:
                    date_str = matches[0]
                    try:
                        date_obj = self._parse_date_string(date_str)
                        return date_obj.strftime("%d/%m/%y")
                    except ValueError:
                        continue
        
        # Strategy 3: Use today's date as fallback
        logger.warning("No date found, using today's date as fallback")
        return datetime.now().strftime("%d/%m/%y")
    
    def _parse_date_string(self, date_str: str) -> datetime:
        """Parse date string with flexible month handling."""
        month_mapping = {
            'jan': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'apr': 'Apr',
            'may': 'May', 'jun': 'Jun', 'jul': 'Jul', 'aug': 'Aug',
            'sep': 'Sep', 'oct': 'Oct', 'nov': 'Nov', 'dec': 'Dec',
            'january': 'Jan', 'february': 'Feb', 'march': 'Mar',
            'april': 'Apr', 'june': 'Jun', 'july': 'Jul',
            'august': 'Aug', 'september': 'Sep', 'october': 'Oct',
            'november': 'Nov', 'december': 'Dec'
        }
        
        for month_lower, month_abbr in month_mapping.items():
            if month_lower in date_str.lower():
                parts = re.split(r'[-./]', date_str)
                if len(parts) >= 2:
                    month_part = parts[1]
                    if month_part.lower() == month_lower:
                        parts[1] = month_abbr
                        date_str = '-'.join(parts)
                        break
        
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
    
    def parse_summary_section_enhanced(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Enhanced summary section parsing."""
        summary_data = []
        
        # Find summary section
        summary_start = -1
        for i, line in enumerate(lines):
            if self.summary_pattern.search(line):
                summary_start = i
                logger.debug(f"Found summary at line {i}: '{line}'")
                break
        
        if summary_start == -1:
            logger.debug("No summary section found")
            return summary_data
        
        # Look for header after summary
        header_found = False
        for i in range(summary_start + 1, min(summary_start + 10, len(lines))):
            line = lines[i].strip()
            if not line:
                continue
            
            if self._is_header_line(line):
                header_found = True
                header_line = i
                logger.debug(f"Found header at line {i}: '{line}'")
                break
        
        if not header_found:
            header_line = summary_start
        
        # Parse fuel data lines after header
        for i in range(header_line + 1, min(header_line + 20, len(lines))):
            line = lines[i].strip()
            if not line:
                continue
            
            if self._is_section_header(line) and i > header_line + 1:
                logger.debug(f"Stopping at section header: '{line}'")
                break
            
            fuel_item = self.parse_fuel_line_enhanced(line)
            if fuel_item:
                summary_data.append(fuel_item)
                logger.debug(f"Parsed summary item: {fuel_item['fuel_type']}")
            elif line and i > header_line + 2:
                if self._looks_like_total_line(line):
                    logger.debug(f"Found total line, stopping: '{line}'")
                    break
        
        logger.info(f"Parsed {len(summary_data)} summary items")
        return summary_data
    
    def parse_fuel_line_enhanced(self, line: str) -> Optional[Dict[str, Any]]:
        """Enhanced fuel line parsing."""
        if not line.strip():
            return None
        
        if self._is_header_line(line):
            return None
        
        match = self.fuel_line_pattern.search(line)
        if match:
            try:
                fuel_type = match.group(1).strip()
                volume_str = match.group(2)
                amount_str = match.group(3)
                
                volume = self._clean_number(volume_str)
                amount = self._clean_number(amount_str)
                
                if volume <= 0:
                    logger.debug(f"Invalid volume ({volume}) for fuel: {fuel_type}")
                    return None
                
                unit_price = amount / volume if volume > 0 else 0.0
                
                return {
                    "fuel_type": fuel_type,
                    "volume": volume,
                    "amount": amount,
                    "unit_price": round(unit_price, 3)
                }
                
            except (ValueError, ZeroDivisionError) as e:
                logger.debug(f"Failed to parse fuel line '{line}': {e}")
                return None
        
        parts = line.split()
        if len(parts) >= 3:
            try:
                fuel_type = ' '.join(parts[:-2])
                volume = self._clean_number(parts[-2])
                amount = self._clean_number(parts[-1])
                
                if volume <= 0:
                    return None
                
                return {
                    "fuel_type": fuel_type,
                    "volume": volume,
                    "amount": amount,
                    "unit_price": amount / volume if volume > 0 else 0.0
                }
            except (ValueError, IndexError):
                pass
        
        return None
    
    def _clean_number(self, num_str: str) -> float:
        """Clean and convert number string to float."""
        if not num_str:
            return 0.0
        
        cleaned = re.sub(r'[^\d\.]', '', str(num_str))
        
        if not cleaned:
            return 0.0
        
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    
    def _is_header_line(self, line: str) -> bool:
        """Check if line looks like a table header."""
        line_lower = line.lower()
        
        header_keywords = ["product", "volume", "amount", "pump", "ស្តង់", "បរិមាណ", "តម្លៃ"]
        
        for keyword in header_keywords:
            if keyword in line_lower and len(line.split()) <= 3:
                return True
        
        parts = re.split(r'\s{2,}|\t', line)
        if len(parts) >= 2 and all(len(p.strip()) < 20 for p in parts):
            return True
        
        return False
    
    def _is_section_header(self, line: str) -> bool:
        """Check if line looks like a section header."""
        section_headers = ["Pump", "Summary", "Total", "សរុប", "ស្តង់"]
        return any(header.lower() in line.lower() for header in section_headers)
    
    def _looks_like_total_line(self, line: str) -> bool:
        """Check if line looks like a total line."""
        total_patterns = ["Total Sale", "Total Sales", "Grand Total", "សរុបការលក់", "សរុបទាំងអស់"]
        return any(pattern.lower() in line.lower() for pattern in total_patterns)
    
    def parse_pump_data_enhanced(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Enhanced pump data parsing."""
        pumps = []
        current_pump = None
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            pump_match = self.pump_pattern.search(line)
            if pump_match:
                if current_pump and current_pump["fuels"]:
                    pumps.append(current_pump)
                    logger.debug(f"Added pump {current_pump['pump_number']}")
                
                pump_number = pump_match.group(1)
                current_pump = {
                    "pump_number": pump_number,
                    "fuels": []
                }
                logger.debug(f"Started pump {pump_number} at line {i}")
                continue
            
            if current_pump:
                fuel_item = self.parse_fuel_line_enhanced(line)
                if fuel_item:
                    current_pump["fuels"].append(fuel_item)
                elif line and i > 0 and self._is_section_header(line):
                    if current_pump["fuels"]:
                        pumps.append(current_pump)
                    current_pump = None
        
        if current_pump and current_pump["fuels"]:
            pumps.append(current_pump)
        
        logger.info(f"Parsed {len(pumps)} pumps")
        return pumps
    
    def aggregate_fuel_from_pumps(self, pumps_data: List[Dict]) -> List[Dict[str, Any]]:
        """Aggregate fuel data from pump data."""
        if not pumps_data:
            return []
        
        fuel_totals = {}
        
        for pump in pumps_data:
            for fuel in pump.get("fuels", []):
                fuel_type = fuel["fuel_type"]
                if fuel_type not in fuel_totals:
                    fuel_totals[fuel_type] = {
                        "volume": 0.0,
                        "amount": 0.0,
                        "pump_count": 0
                    }
                
                fuel_totals[fuel_type]["volume"] += fuel.get("volume", 0)
                fuel_totals[fuel_type]["amount"] += fuel.get("amount", 0)
                fuel_totals[fuel_type]["pump_count"] += 1
        
        fuel_data = []
        for fuel_type, totals in fuel_totals.items():
            fuel_data.append({
                "fuel_type": fuel_type,
                "volume": totals["volume"],
                "amount": totals["amount"],
                "pump_count": totals["pump_count"]
            })
        
        logger.info(f"Aggregated {len(fuel_data)} fuel types from pumps")
        return fuel_data
    
    def extract_fuel_data_direct(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Extract fuel data directly from lines."""
        fuel_totals = {}
        
        for line in lines:
            fuel_item = self.parse_fuel_line_enhanced(line)
            if fuel_item:
                fuel_type = fuel_item["fuel_type"]
                if fuel_type not in fuel_totals:
                    fuel_totals[fuel_type] = {"volume": 0.0, "amount": 0.0}
                
                fuel_totals[fuel_type]["volume"] += fuel_item["volume"]
                fuel_totals[fuel_type]["amount"] += fuel_item["amount"]
        
        fuel_data = []
        for fuel_type, totals in fuel_totals.items():
            fuel_data.append({
                "fuel_type": fuel_type,
                "volume": totals["volume"],
                "amount": totals["amount"]
            })
        
        logger.info(f"Extracted {len(fuel_data)} fuel types directly")
        return fuel_data
    
    def extract_total_sales_enhanced(self, lines: List[str]) -> Dict[str, float]:
        """Enhanced total sales extraction."""
        total_sales = {"volume": 0.0, "amount": 0.0}
        
        for line in lines:
            match = self.total_sale_pattern.search(line)
            if match:
                try:
                    volume = self._clean_number(match.group(1))
                    amount = self._clean_number(match.group(2))
                    
                    total_sales["volume"] = volume
                    total_sales["amount"] = amount
                    
                    logger.info(f"Extracted total sales: {volume:.2f}L, ${amount:.2f}")
                    return total_sales
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse total sales from '{line}': {e}")
        
        for line in lines:
            if "total" in line.lower() or "សរុប" in line:
                numbers = self.number_pattern.findall(line)
                if len(numbers) >= 2:
                    try:
                        total_sales["volume"] = self._clean_number(numbers[0])
                        total_sales["amount"] = self._clean_number(numbers[1])
                        logger.info(f"Extracted total sales from numbers: {total_sales['volume']:.2f}L")
                        return total_sales
                    except (ValueError, IndexError):
                        continue
        
        logger.debug("No total sales found")
        return total_sales
    
    def map_fuel_types_enhanced(self, fuel_data: List[Dict]) -> List[Dict]:
        """Enhanced fuel type mapping."""
        mapped_data = []
        
        for item in fuel_data:
            fuel_type = item["fuel_type"].strip()
            original_type = fuel_type
            
            mapped_type = self.fuel_mappings.get(fuel_type)
            
            if not mapped_type:
                mapped_type = self.fuel_mappings.get(fuel_type.lower())
            
            if not mapped_type:
                best_match = None
                best_score = 0
                
                for key, value in self.fuel_mappings.items():
                    score = SequenceMatcher(None, fuel_type.lower(), key.lower()).ratio()
                    
                    if key.lower() in fuel_type.lower() or fuel_type.lower() in key.lower():
                        score += 0.3
                    
                    if score > best_score and score > 0.6:
                        best_score = score
                        best_match = value
                
                if best_match:
                    mapped_type = best_match
                    logger.debug(f"Partial match '{fuel_type}' -> '{key}' (score: {best_score:.2f})")
            
            if not mapped_type:
                fuel_lower = fuel_type.lower()
                if any(word in fuel_lower for word in ["diesel", "do", "ម៉ាស៊ូត"]):
                    mapped_type = "ប្រេងម៉ាស៊ូត DO - T1"
                elif any(word in fuel_lower for word in ["regular", "ea92", "សាំង", "92"]):
                    mapped_type = "សាំង EA92 - T2"
                elif any(word in fuel_lower for word in ["super", "ea95", "premium", "95"]):
                    mapped_type = "សាំងស៊ុបពែរ EA95 -T3"
                else:
                    mapped_type = re.sub(r'\s+', ' ', fuel_type).strip()
                    logger.debug(f"No mapping for '{fuel_type}', keeping original")
            
            mapped_item = {
                "fuel_type": mapped_type,
                "original_fuel_type": original_type,
                "volume": item.get("volume", 0.0),
                "amount": item.get("amount", 0.0),
                "unit_price": item.get("unit_price", 0.0)
            }
            
            for key in ["pump_count", "pump_number"]:
                if key in item:
                    mapped_item[key] = item[key]
            
            mapped_data.append(mapped_item)
            
            if original_type != mapped_type:
                logger.debug(f"Mapped: '{original_type}' -> '{mapped_type}'")
        
        return mapped_data
    
    def verify_and_calculate_totals(self, result: Dict[str, Any]) -> None:
        """Verify and calculate totals for consistency."""
        fuel_data = result.get("fuel_data", [])
        total_sales = result.get("total_sales", {})
        
        if not fuel_data:
            return
        
        calculated_volume = sum(item.get("volume", 0) for item in fuel_data)
        calculated_amount = sum(item.get("amount", 0) for item in fuel_data)
        
        if total_sales.get("volume", 0) == 0 and total_sales.get("amount", 0) == 0:
            total_sales["volume"] = calculated_volume
            total_sales["amount"] = calculated_amount
            logger.info(f"Using calculated totals: {calculated_volume:.2f}L, ${calculated_amount:.2f}")
            return
        
        if calculated_volume > 0 and total_sales["volume"] > 0:
            volume_diff = abs(calculated_volume - total_sales["volume"])
            volume_diff_pct = (volume_diff / total_sales["volume"]) * 100
            
            amount_diff = abs(calculated_amount - total_sales["amount"])
            amount_diff_pct = (amount_diff / total_sales["amount"]) * 100 if total_sales["amount"] > 0 else 100
            
            tolerance = 2.0
            
            if volume_diff_pct > tolerance or amount_diff_pct > tolerance:
                logger.warning(
                    f"Totals mismatch: "
                    f"Volume: {calculated_volume:.2f}L vs {total_sales['volume']:.2f}L ({volume_diff_pct:.1f}% diff) "
                    f"Amount: ${calculated_amount:.2f} vs ${total_sales['amount']:.2f} ({amount_diff_pct:.1f}% diff)"
                )
                
                if volume_diff_pct > 10 or amount_diff_pct > 10:
                    total_sales["volume"] = calculated_volume
                    total_sales["amount"] = calculated_amount
                    logger.info("Using calculated totals due to large mismatch")
    
    def validate_parsed_data_enhanced(self, parsed_data: Dict) -> Dict[str, Any]:
        """Enhanced validation of parsed data."""
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "checks_passed": 0,
            "checks_total": 0
        }
        
        try:
            checks = [
                self._check_required_fields,
                self._check_station_name,
                self._check_report_date,
                self._check_fuel_data,
                self._check_totals_consistency,
            ]
            
            for check_func in checks:
                check_result = check_func(parsed_data)
                validation_result["checks_total"] += check_result.get("total", 1)
                validation_result["checks_passed"] += check_result.get("passed", 0)
                
                if check_result.get("errors"):
                    validation_result["errors"].extend(check_result["errors"])
                    validation_result["is_valid"] = False
                
                if check_result.get("warnings"):
                    validation_result["warnings"].extend(check_result["warnings"])
            
            if validation_result["checks_total"] > 0:
                validation_result["score"] = (
                    validation_result["checks_passed"] / validation_result["checks_total"]
                ) * 100
            else:
                validation_result["score"] = 0
            
            logger.info(f"Validation: {validation_result['checks_passed']}/{validation_result['checks_total']} "
                       f"({validation_result['score']:.1f}%)")
            
            return validation_result
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
            return validation_result
    
    def _check_required_fields(self, data: Dict) -> Dict[str, Any]:
        """Check required fields."""
        result = {"passed": 0, "total": 3, "errors": [], "warnings": []}
        
        required = ["station_name", "report_date", "fuel_data"]
        for field in required:
            if field in data and data[field]:
                result["passed"] += 1
            else:
                result["errors"].append(f"Missing required field: {field}")
        
        return result
    
    def _check_station_name(self, data: Dict) -> Dict[str, Any]:
        """Check station name validity."""
        result = {"passed": 0, "total": 1, "errors": [], "warnings": []}
        
        station = data.get("station_name", "")
        if station and station not in ["Unknown Station", "UNKNOWN"]:
            result["passed"] = 1
        else:
            result["errors"].append("Invalid station name")
        
        return result
    
    def _check_report_date(self, data: Dict) -> Dict[str, Any]:
        """Check report date format."""
        result = {"passed": 0, "total": 1, "errors": [], "warnings": []}
        
        date_str = data.get("report_date", "")
        try:
            datetime.strptime(date_str, "%d/%m/%y")
            result["passed"] = 1
        except ValueError:
            result["warnings"].append(f"Invalid date format: {date_str}")
        
        return result
    
    def _check_fuel_data(self, data: Dict) -> Dict[str, Any]:
        """Check fuel data quality."""
        result = {"passed": 0, "total": 2, "errors": [], "warnings": []}
        
        fuel_data = data.get("fuel_data", [])
        if fuel_data:
            result["passed"] += 1
            
            has_negative = False
            for fuel in fuel_data:
                if fuel.get("volume", 0) < 0 or fuel.get("amount", 0) < 0:
                    has_negative = True
                    break
            
            if not has_negative:
                result["passed"] += 1
            else:
                result["warnings"].append("Negative values in fuel data")
        else:
            result["errors"].append("No fuel data")
        
        return result
    
    def _check_totals_consistency(self, data: Dict) -> Dict[str, Any]:
        """Check totals consistency."""
        result = {"passed": 0, "total": 1, "errors": [], "warnings": []}
        
        fuel_data = data.get("fuel_data", [])
        total_sales = data.get("total_sales", {})
        
        if fuel_data and total_sales.get("volume", 0) > 0:
            calculated = sum(f.get("volume", 0) for f in fuel_data)
            reported = total_sales.get("volume", 0)
            
            if reported > 0:
                diff_pct = abs(calculated - reported) / reported * 100
                if diff_pct <= 5:
                    result["passed"] = 1
                else:
                    result["warnings"].append(
                        f"Volume mismatch: calculated {calculated:.2f}L vs reported {reported:.2f}L "
                        f"({diff_pct:.1f}% diff)"
                    )
        
        return result
    
    def _create_error_result(self, error_message: str) -> Dict[str, Any]:
        """Create error result structure."""
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
                "is_valid": False,
                "validation": {
                    "is_valid": False,
                    "errors": [error_message],
                    "score": 0
                }
            }
        }
    
    def format_for_database(self, parsed_data: Dict) -> Dict:
        """Format parsed data for database storage."""
        try:
            validation = parsed_data.get("metadata", {}).get("validation", {})
            
            db_data = {
                "station_name": parsed_data.get("station_name", "UNKNOWN"),
                "report_date": parsed_data.get("report_date", ""),
                "fuel_data": parsed_data.get("fuel_data", []),
                "total_volume": parsed_data.get("total_sales", {}).get("volume", 0.0),
                "total_amount": parsed_data.get("total_sales", {}).get("amount", 0.0),
                "pump_count": len(parsed_data.get("pump_data", [])),
                "parsed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "validation_score": validation.get("score", 0),
                "is_valid": validation.get("is_valid", False),
                "validation_warnings": validation.get("warnings", []),
                "parsing_method": parsed_data.get("metadata", {}).get("parsing_method", "unknown"),
                "station_format": parsed_data.get("metadata", {}).get("station_format_detected", "unknown"),
                "station_confidence": parsed_data.get("metadata", {}).get("station_confidence", 0.0)
            }
            
            fuel_data = parsed_data.get("fuel_data", [])
            if fuel_data:
                db_data["fuel_types_count"] = len(fuel_data)
                if db_data["total_volume"] > 0:
                    db_data["average_price"] = db_data["total_amount"] / db_data["total_volume"]
            
            logger.info(f"Formatted for DB: {db_data['station_name']} - {db_data['report_date']} "
                       f"(Score: {db_data['validation_score']:.1f}%)")
            
            return db_data
            
        except Exception as e:
            logger.error(f"Error formatting for database: {e}")
            return {
                "station_name": parsed_data.get("station_name", "UNKNOWN"),
                "report_date": parsed_data.get("report_date", datetime.now().strftime("%d/%m/%y")),
                "fuel_data": parsed_data.get("fuel_data", []),
                "total_volume": parsed_data.get("total_sales", {}).get("volume", 0.0),
                "total_amount": parsed_data.get("total_sales", {}).get("amount", 0.0),
                "parsed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "is_valid": False,
                "error": str(e)
            }


# Backward compatibility
def parse_daily_report(report_text: str) -> Dict[str, Any]:
    parser = ReportParser()
    return parser.parse_daily_report(report_text)

def format_for_database(parsed_data: Dict) -> Dict:
    parser = ReportParser()
    return parser.format_for_database(parsed_data)


# Test function specific to your format
def test_bvm_format_parser():
    """Test the parser with BVM format reports."""
    print("=" * 70)
    print("BVM FORMAT PARSER TEST")
    print("=" * 70)
    
    test_reports = [
        {
            "name": "Standard BVM Khmer",
            "report": """BVM ព្រែកអញ្ចាញ
Summary Report
27-Dec-2025 12:00 AM to 27-Dec-2025 11:59 PM
Product	Volume	Amount
Pump 1
Diesel	309.82	272.65
Regular	277.19	260.56
Super	166.13	179.41
Summary
Product	Volume	Amount
Diesel	790.26	695.45
Regular	1798.43	1690.48
Super	543.15	586.53
Total Sale	3131.84	2972.46"""
        },
        {
            "name": "BVM with location only",
            "report": """BVM កោះនរា
26-Nov-2025
Diesel	100	90
Regular	200	180
Super	50	55
Total	350	325"""
        },
        {
            "name": "BMV variation",
            "report": """BMV ផ្សារចាស់
Daily Report
28/12/25
Pump 1
Diesel	150	135
Regular	250	225
Total	400	360"""
        },
        {
            "name": "With station prefix in Khmer",
            "report": """ស្ថានីយ៍ BVM ទួលគោក
Summary
29-Dec-2025
Diesel	200	180
Regular	300	270
Super	100	110
Total Sale	600	560"""
        }
    ]
    
    parser = ReportParser()
    
    for test in test_reports:
        print(f"\n{'='*40}")
        print(f"TEST: {test['name']}")
        print(f"{'='*40}")
        
        result = parser.parse_daily_report(test['report'])
        
        if result and result.get("station_name"):
            print(f"✅ Station: {result['station_name']}")
            print(f"📅 Date: {result['report_date']}")
            print(f"📊 Format: {result['metadata'].get('station_format_detected', 'N/A')}")
            print(f"🎯 Confidence: {result['metadata'].get('station_confidence', 0):.2f}")
            print(f"⛽ Fuel types: {len(result['fuel_data'])}")
            print(f"📈 Total volume: {result['total_sales']['volume']:.2f}L")
            
            if result['fuel_data']:
                print("\nFuel breakdown:")
                for fuel in result['fuel_data']:
                    print(f"  {fuel['fuel_type']}: {fuel['volume']:.2f}L (${fuel['amount']:.2f})")
        else:
            print("❌ Failed to parse report")


if __name__ == "__main__":
    test_bvm_format_parser()
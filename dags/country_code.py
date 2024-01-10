import phonenumbers as pn
import iso3166

def create_region_mapping():
    countries = iso3166.countries_by_alpha2

    region_mapping = {}
    for country_code, country_info in countries.items():
        try:
            region_mapping[country_info.name] = country_code
        except Exception as e:
            print(f"Error processing country {country_info.name}: {e}")

    return region_mapping

def fix_phone(phone, country):
    region_mapping = create_region_mapping()

    normalized_region = region_mapping.get(country, country)
    phone = ''.join(filter(lambda x: x.isdigit() or x.lower() == 'x', phone))
    phone = phone.replace("(", "").replace(")", "").replace(" ", "")

    try:
        if phone.startswith('00'):
            phone = '+' + phone[2:]

        if 'x' in phone:
            phone = phone.split('x')[0]

        parsed_number = pn.parse(phone, normalized_region)
        formatted_number = pn.format_number(parsed_number, pn.PhoneNumberFormat.INTERNATIONAL)
        result = f"+{normalized_region} {formatted_number}"
        result = result.replace(f"+{normalized_region} ", "")

        return result
    except pn.NumberParseException as e:
        print(f"An error occurred for country {country}: {e}. Adding '+' to the phone number.")
        return f"+{phone}"
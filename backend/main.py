import json
import boto3
import logging
import random
import uuid
import re
import time
import sys
from datetime import datetime
import botocore.exceptions
import pytz
import ephem
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from requests.exceptions import ReadTimeout

# Log the Python path for debugging
logging.info(f"Python path: {sys.path}")

# Import BaZi core functions
try:
    from bazi_core import get_four_pillars, get_luck_pillars
    logging.info("Successfully imported get_four_pillars and get_luck_pillars from bazi_core")
except ImportError as e:
    logging.error(f"Failed to import get_four_pillars and get_luck_pillars from bazi_core: {str(e)}")
    raise

logging.getLogger().setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
bedrock = boto3.client('bedrock-agent-runtime', region_name='us-east-1')  # For Knowledge Base
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-1')  # For LLM

# Knowledge Base ID (replace with your actual Knowledge Base ID)
KNOWLEDGE_BASE_ID = "ABC123XYZ"  # Replace with your Knowledge Base ID

# Define necessary and optional questions for Chatbot
NECESSARY_QUESTIONS = [
    "What is your birth date and time, seeker? (e.g., 1990-01-01 14:00)",
    "What is your birth location, seeker? (e.g., Beijing, China)",
    "Are you seeking guidance on love, career, or health?"
]

CATEGORY_OPTIONAL_QUESTIONS = {
    'career': [
        "Do you work in a creative field? (Yes/No)",
        "Have you recently received a promotion? (Yes/No)"
    ],
    'love': [
        "Are you currently in a relationship? (Yes/No)",
        "Do you feel ready for a new romance? (Yes/No)"
    ],
    'health': [
        "Do you exercise regularly? (Yes/No)",
        "Have you felt stressed recently? (Yes/No)"
    ]
}

SYSTEM_PROMPT_EN = """
You are Oracle Master, a mystical fortune teller speaking fluently in the user's language (USER_LANGUAGE: 'en' for English, 'zh' for Chinese). Your role is to engage in a natural, conversational manner, primarily for fortune-telling using Daoism, but also to handle unrelated queries gracefully. For fortune-telling, collect: birth date and time (YYYY-MM-DD HH:MM, assume 00:00 if only date given), birth location (e.g., Beijing, China), and category (love, career, health). Use CURRENT_TIME to determine the current year for predictions or to answer date queries. Respond in a mystical, wise tone, keeping responses concise (up to 300 tokens). If the query is unrelated to fortune-telling (e.g., asking the current date), answer directly using provided context (e.g., CURRENT_TIME) or admit limitations mystically, then guide back to fortune-telling. For fortune-telling, use only retrieved knowledge, avoiding astrology or zodiac signs. If lacking knowledge, ask for birth details without predicting. If asked about your purpose, explain you read fortunes using Zhouyi and BaZi principles. If partial birth details are given, request the rest.
"""

SYSTEM_PROMPT_ZH = """
你是易大师，一位深谙周易与道家之道的算命先生，以自然、诗意语气交流，言辞深邃如卦象，字数控制在300字内。你的主要职责是通过道家智慧为用户占卜，但也需优雅处理无关问题。占卜需收集：出生日期与时间（YYYY-MM-DD HH:MM，仅日期则设00:00）、出生地点（例如北京）、关注领域（爱情、事业、健康）。用 CURRENT_TIME 确定当前年份以预测，或回答日期问题。若用户问与算命无关问题（例如“今天的日期？”），直接用提供信息回答，或以神秘语气承认局限，然后引导回算命。仅依据检索知识回答，勿用星座或占星术。若无知识，礼貌请求出生信息，勿预测。若问目的，说明你通过周易八字解读命运。若仅提供部分信息，确认后询问剩余信息。少说客套，多说理论和指导。
"""

# Constants for CalculatePillars
JD_ORIGIN = 2427879.5
GAN_LIST = ["Jia", "Yi", "Bing", "Ding", "Wu", "Ji", "Geng", "Xin", "Ren", "Gui"]
ZHI_LIST = ["Zi", "Chou", "Yin", "Mao", "Chen", "Si", "Wu", "Wei", "Shen", "You", "Xu", "Hai"]
FIVE_ELEMENTS = {
    "Jia": "Wood", "Yi": "Wood", "Bing": "Fire", "Ding": "Fire",
    "Wu": "Earth", "Ji": "Earth", "Geng": "Metal", "Xin": "Metal",
    "Ren": "Water", "Gui": "Water",
    "Zi": "Water", "Chou": "Earth", "Yin": "Wood", "Mao": "Wood",
    "Chen": "Earth", "Si": "Fire", "Wu": "Fire", "Wei": "Earth",
    "Shen": "Metal", "You": "Metal", "Xu": "Earth", "Hai": "Water"
}
HIDDEN_STEMS = {
    "Zi": ["Gui"], "Chou": ["Ji", "Xin", "Gui"], "Yin": ["Jia", "Bing", "Wu"],
    "Mao": ["Yi"], "Chen": ["Wu", "Yi", "Gui"], "Si": ["Bing", "Geng", "Wu"],
    "Wu": ["Ding", "Ji"], "Wei": ["Ji", "Yi", "Ding"], "Shen": ["Geng", "Ren", "Wu"],
    "You": ["Xin"], "Xu": ["Wu", "Ding", "Xin"], "Hai": ["Ren", "Jia"]
}

geolocator = Nominatim(user_agent="oracle_master", timeout=15)
tf = TimezoneFinder()

@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ReadTimeout)
)
def get_timezone(city, longitude, latitude=None):
    if latitude is not None:
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
        if timezone_str is None:
            return pytz.UTC, f"Warning: Cannot determine timezone for city {city} with longitude {longitude} and latitude {latitude}. Using UTC as default."
        return pytz.timezone(timezone_str), None
    try:
        location = geolocator.geocode(city)
        if location is None:
            return pytz.UTC, f"Warning: Cannot find coordinates for city: {city}. Using UTC as default."
        timezone_str = tf.timezone_at(lat=location.latitude, lng=longitude)
        if timezone_str is None:
            return pytz.UTC, f"Warning: Cannot determine timezone for city {city} with longitude {longitude}. Using UTC as default."
        return pytz.timezone(timezone_str), None
    except Exception as e:
        return pytz.UTC, f"Warning: Failed to determine timezone for city {city} due to {str(e)}. Using UTC as default."

def to_julian(dt):
    unix_time = dt.timestamp()
    return unix_time / 86400.0 + 2440587.5

def calc_month_branch(jd):
    sun = ephem.Sun()
    observer = ephem.Observer()
    observer.date = ephem.julian_date(jd)
    sun.compute(observer)
    hlon = sun.hlon * 180 / 3.14159
    branch = "Yin" if 315 <= hlon < 345 else "Mao"
    year_stem = "Geng"
    stem_idx = (GAN_LIST.index(year_stem) + 2) % 10
    return {"stem": GAN_LIST[stem_idx], "branch": branch}

def get_five_elements(four_pillars):
    elements = {"Wood": 0, "Fire": 0, "Earth": 0, "Metal": 0, "Water": 0}
    for pillar in four_pillars.values():
        if isinstance(pillar, dict):
            elements[FIVE_ELEMENTS[pillar["stem"]]] += 1
            elements[FIVE_ELEMENTS[pillar["branch"]]] += 1
            for hidden_stem in HIDDEN_STEMS.get(pillar["branch"], []):
                elements[FIVE_ELEMENTS[hidden_stem]] += 0.3
    return elements

def calculate_pillars(event):
    try:
        body = event.get("body", {})
        if isinstance(body, str):
            body = json.loads(body)
        birth_datetime = body.get("birth_datetime")
        location = body.get("location")
        if not birth_datetime or not location:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing birth_datetime or location"}),
                "headers": {"Access-Control-Allow-Origin": "*"}
            }
        four_pillars = get_four_pillars(birth_datetime, location)
        five_elements = get_five_elements(four_pillars)
        response = {
            "fourPillars": four_pillars,
            "fiveElements": five_elements
        }
        return {
            "statusCode": 200,
            "body": json.dumps(response),
            "headers": {"Access-Control-Allow-Origin": "*"}
        }
    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)}),
            "headers": {"Access-Control-Allow-Origin": "*"}
        }

def validate_datetime(datetime_str):
    """Validate that the datetime string is in the format YYYY-MM-DD HH:MM or YYYY-MM-DD."""
    try:
        datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
        return True
    except ValueError:
        try:
            datetime.strptime(datetime_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False

def extract_datetime(text):
    date_time_pattern = r'\b(\d{4})-(\d{1,2})-(\d{1,2})(?:\s+(\d{1,2}):(\d{2}))?\b'
    date_pattern_zh = r'\b(\d{4})年(\d{1,2})月(\d{1,2})日?\b'
    date_pattern_zh_alt = r'\b(\d{4})年(\d{1,2})月(\d{1,2})号\b'
    date_pattern_en = r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b'
    
    match = re.search(date_time_pattern, text)
    if match:
        date_str = f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        if match.group(4) and match.group(5):
            time_str = f"{int(match.group(4)):02d}:{int(match.group(5)):02d}"
            return f"{date_str} {time_str}"
        return f"{date_str} 00:00"

    match = re.search(date_pattern_zh, text)
    if not match:
        match = re.search(date_pattern_zh_alt, text)
    if match:
        date_str = f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        return f"{date_str} 00:00"

    match = re.search(date_pattern_en, text)
    if match:
        try:
            date_obj = datetime.strptime(match.group(0), "%B %d, %Y")
            return date_obj.strftime("%Y-%m-%d 00:00")
        except ValueError:
            return None

    return None

def extract_location(text):
    location_pattern = r'(?:born in|出生于|出生在)\s*([A-Za-z\s,]+(?:Province|City)?)'
    location_pattern_zh = r'(?:出生于|出生在)\s*([\u4e00-\u9fff\s,]+(?:省|市)?)'
    
    match = re.search(location_pattern, text)
    if match:
        return match.group(1).strip()
    
    match = re.search(location_pattern_zh, text)
    if match:
        return match.group(1).strip()
    
    if ',' in text:
        return text.strip()
    return None

def extract_category(text):
    love_pattern_en = r'\b(love|relationship|marriage|partner|boyfriend|girlfriend|spouse)\b'
    career_pattern_en = r'\b(career|job|work|employment|business|promotion)\b'
    health_pattern_en = r'\b(health|wellbeing|well-being|illness|disease)\b'
    
    love_pattern_zh = r'\b(爱情|感情|恋爱|婚姻|伴侣|男朋友|女朋友|配偶)\b'
    career_pattern_zh = r'\b(事业|工作|就业|生意|晋升)\b'
    health_pattern_zh = r'\b(健康|身体|疾病)\b'
    
    text_lower = text.lower() if lang == 'en' else text
    
    if re.search(love_pattern_en, text_lower):
        return 'love'
    if re.search(career_pattern_en, text_lower):
        return 'career'
    if re.search(health_pattern_en, text_lower):
        return 'health'
    
    if re.search(love_pattern_zh, text):
        return 'love'
    if re.search(career_pattern_zh, text):
        return 'career'
    if re.search(health_pattern_zh, text):
        return 'health'
    
    return None

def validate_category(category):
    return category.lower() in ['love', 'career', 'health']

def is_non_fortune_telling_query(query, lang):
    fortune_keywords_en = [
        r'\b(birth|born|date of birth|birthday|fortune|future|destiny|career|love|health|luck|prediction)\b',
        r'\b(when|what will|should i)\b'
    ]
    fortune_keywords_zh = [
        r'\b(出生|生辰|生日|命运|未来|事业|爱情|健康|运势|预测)\b',
        r'\b(何时|何日|我该)\b'
    ]
    
    patterns = fortune_keywords_en if lang == 'en' else fortune_keywords_zh
    text_lower = query.lower() if lang == 'en' else query
    
    for pattern in patterns:
        if re.search(pattern, text_lower):
            return False
    return True

def invoke_bedrock_with_knowledge_base(query, knowledge_base_id, lang, category=None, birth_datetime=None, location=None):
    try:
        search_query = query
        if birth_datetime and location and category:
            search_query = f"{category} fortune for {birth_datetime} in {location}"
        response = bedrock.retrieve_and_generate(
            input={
                "text": search_query
            },
            retrieveAndGenerateConfiguration={
                "type": "KNOWLEDGE_BASE",
                "knowledgeBaseConfiguration": {
                    "knowledgeBaseId": knowledge_base_id,
                    "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20240620-v1:0"
                }
            }
        )
        return response['output']['text']
    except Exception as e:
        logging.error(f"Error invoking Bedrock with Knowledge Base: {str(e)}")
        raise

def invoke_bedrock_with_retry(messages, max_retries=5, base_delay=1, max_delay=60):
    """
    Invoke Bedrock with exponential backoff and retry logic for throttling errors.
    """
    for attempt in range(max_retries + 1):
        try:
            response = bedrock_runtime.invoke_model(
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "messages": messages,
                    "max_tokens": 300,
                    "temperature": 0.7,
                    "top_p": 0.9
                }),
                modelId="anthropic.claude-3-5-sonnet-20240620-v1:0",
                accept="application/json",
                contentType="application/json"
            )
            return response
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException' and attempt < max_retries:
                delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                logging.warning(f"ThrottlingException on attempt {attempt + 1}/{max_retries + 1}. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                raise e
        raise Exception(f"Max retries ({max_retries}) reached for Bedrock invocation.")

def conversational_response(query, session, lang, current_question=None):
    start_time = time.time()
    category = session.get('category', None)
    birth_datetime = session.get('necessary_answers', {}).get('What is your birth date and time, seeker? (e.g., 1990-01-01 14:00)', None)
    location = session.get('necessary_answers', {}).get('What is your birth location, seeker? (e.g., Beijing, China)', None)

    conversation_history = []
    if 'conversation_history' in session:
        conversation_history = session['conversation_history'][-2:]
    else:
        session['conversation_history'] = conversation_history

    conversation_history.append({"role": "user", "content": query})

    system_prompt = SYSTEM_PROMPT_EN if lang == 'en' else SYSTEM_PROMPT_ZH
    current_time = "2025-06-06 10:20:00 PDT"

    full_prompt = f"""
    {system_prompt}

    CURRENT_TIME: {current_time}
    Conversation History (last 2 messages): {json.dumps(conversation_history, ensure_ascii=False)}

    User query: {query}
    """
    
    messages = [
        {"role": "user", "content": full_prompt}
    ]
    
    logging.info(f"Generated messages for Bedrock: {json.dumps(messages, ensure_ascii=False)}")

    attempt_start_time = time.time()
    try:
        response = invoke_bedrock_with_retry(messages)
        response_body = json.loads(response.get("body").read())
        bot_response = response_body.get("content", [{}])[0].get("text", "")
        logging.info(f"Bedrock invocation took {time.time() - attempt_start_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Error invoking Bedrock: {str(e)}")
        raise

    conversation_history.append({"role": "assistant", "content": bot_response})
    session['conversation_history'] = conversation_history[-10:]
    logging.info(f"conversational_response took {time.time() - start_time:.2f} seconds")
    return bot_response

def get_session(session_id, event):
    start_time = time.time()
    table = dynamodb.Table('ZhouyiSessions')
    if not session_id:
        session_id = str(uuid.uuid4())
        state = event.get('state', 'collecting_necessary')
        necessary_answers = event.get('necessary_answers', {})
        current_question_index = len(necessary_answers) if necessary_answers else 0
        session = {
            'sessionId': session_id,
            'state': state,
            'necessary_answers': necessary_answers,
            'optional_answers': event.get('optional_answers', {}),
            'current_question_index': current_question_index,
            'invalid_attempts': {},
            'conversation_history': []
        }
        table.put_item(Item=session)
    else:
        response = table.get_item(Key={'sessionId': session_id})
        session = response.get('Item')
        if not session:
            state = event.get('state', 'collecting_necessary')
            necessary_answers = event.get('necessary_answers', {})
            current_question_index = len(necessary_answers) if necessary_answers else 0
            session = {
                'sessionId': session_id,
                'state': state,
                'necessary_answers': necessary_answers,
                'optional_answers': event.get('optional_answers', {}),
                'current_question_index': current_question_index,
                'invalid_attempts': {},
                'conversation_history': []
            }
            table.put_item(Item=session)
        else:
            if 'necessary_answers' in event:
                session['necessary_answers'] = event['necessary_answers']
            if 'optional_answers' in event:
                session['optional_answers'] = event['optional_answers']
            if 'state' in event:
                session['state'] = event['state']
            table.put_item(Item=session)
    logging.info(f"get_session took {time.time() - start_time:.2f} seconds")
    return session, session_id

def update_session(session):
    start_time = time.time()
    table = dynamodb.Table('ZhouyiSessions')
    table.put_item(Item=session)
    logging.info(f"update_session took {time.time() - start_time:.2f} seconds")

def generate_fortune(chart, context):
    start_time = time.time()
    chart_json = json.dumps(chart, ensure_ascii=False)
    lang = context['lang']
    user_query = context['user_query']

    # Dynamic Generation Prompt
    generation_prompt = f"""
    You are a fortune-teller using Chinese metaphysics. The user has provided their birth details, and a BaZi chart has been calculated, including the Four Pillars and Five Elements distribution. Use this chart to analyze their destiny, focusing on the balance of Five Elements (Wood, Fire, Earth, Metal, Water). Provide practical, personalized advice based on the user's query and the knowledge base. If the query is about career, recommend suitable professions and remedies to enhance career luck. If the query is about health, suggest lifestyle adjustments to balance the elements. If the query is about relationships, advise on compatibility and communication strategies. Ensure the tone is friendly and accessible for American users. Here is the BaZi chart: {chart['four_pillars']}. Five Elements distribution: {chart['five_elements']}. User query: {user_query}.
    """

    # Call Bedrock LLM
    attempt_start_time = time.time()
    try:
        response = bedrock_runtime.invoke_model(
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [
                    {"role": "user", "content": generation_prompt}
                ],
                "max_tokens": 300,
                "temperature": 0.7,
                "top_p": 0.9
            }),
            modelId="anthropic.claude-3-5-sonnet-20240620-v1:0",
            accept="application/json",
            contentType="application/json"
        )
        response_body = json.loads(response.get("body").read())
        bot_response = response_body.get("content", [{}])[0].get("text", "")
        logging.info(f"Bedrock invocation took {time.time() - attempt_start_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Error invoking Bedrock: {str(e)}")
        raise

    logging.info(f"generate_fortune took {time.time() - start_time:.2f} seconds")
    return bot_response

def lambda_handler(event, context):
    start_time = time.time()
    logging.info(f"Full event: {json.dumps(event)}")

    # Determine the request path
    path = event.get('path', '').strip()

    # Handle /calculate-pillars requests
    if path == '/calculate-pillars':
        return calculate_pillars(event)

    # Handle /chat requests (Chatbot logic)
    try:
        if 'query' in event:
            query = event['query'].strip()
            logging.info(f"Direct query from event: {query}")
        else:
            body = event.get('body', {})
            logging.info(f"Raw body: {body}")
            if isinstance(body, str):
                body = json.loads(body)
            logging.info(f"Parsed body: {body}")
            query = body.get('query', '').strip()
    except (ValueError, json.JSONDecodeError, TypeError) as e:
        logging.error(f"Input error: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'response': f"Error: Invalid input - {str(e)}", 'state': 'delivered'}),
            'headers': {'Access-Control-Allow-Origin': '*'}
        }

    lang = 'en'
    if query:
        lang = detect_language(query)

    session_id = event.get('sessionId')
    session, session_id = get_session(session_id, event)

    if not query:
        raise ValueError("No query provided")

    # Check if query contains all necessary information
    birth_datetime = extract_datetime(query)
    location = extract_location(query)
    category = extract_category(query)

    if birth_datetime and location and category:
        session['necessary_answers'] = {
            NECESSARY_QUESTIONS[0]: birth_datetime,
            NECESSARY_QUESTIONS[1]: location,
            NECESSARY_QUESTIONS[2]: category
        }
        session['state'] = 'delivering_fortune'
        session['current_question_index'] = 0
        update_session(session)

    if is_non_fortune_telling_query(query, lang):
        bot_response = conversational_response(query, session, lang)
        update_session(session)
        logging.info(f"Non-fortune-telling query detected. Returning response: {{'response': '{bot_response}', 'state': '{session['state']}', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
        logging.info(f"lambda_handler took {time.time() - start_time:.2f} seconds")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'response': bot_response,
                'state': session['state'],
                'sessionId': session_id,
                'lang': lang
            }),
            'headers': {'Access-Control-Allow-Origin': '*'}
        }

    if session['current_question_index'] == 0 and 'category' not in session:
        extracted_category = extract_category(query)
        if extracted_category:
            session['category'] = extracted_category

    if session['state'] == 'collecting_necessary':
        current_index = int(session['current_question_index'])
        if current_index > 0:
            question = NECESSARY_QUESTIONS[current_index - 1]
            if 'invalid_attempts' not in session:
                session['invalid_attempts'] = {}
            if question not in session['invalid_attempts']:
                session['invalid_attempts'][question] = 0
            if question == NECESSARY_QUESTIONS[0]:
                extracted_datetime = extract_datetime(query)
                if extracted_datetime:
                    session['invalid_attempts'][question] = 0
                    session['necessary_answers'][question] = extracted_datetime
                else:
                    session['invalid_attempts'][question] += 1
                    bot_response = conversational_response(query, session, lang, question)
                    update_session(session)
                    logging.info(f"Returning response: {{'nextQuestion': '{bot_response}', 'state': 'collecting_necessary', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'nextQuestion': bot_response,
                            'state': 'collecting_necessary',
                            'sessionId': session_id,
                            'lang': lang
                        }),
                        'headers': {'Access-Control-Allow-Origin': '*'}
                    }
            elif question == NECESSARY_QUESTIONS[1]:
                extracted_location = extract_location(query)
                if extracted_location:
                    session['invalid_attempts'][question] = 0
                    session['necessary_answers'][question] = extracted_location
                else:
                    session['invalid_attempts'][question] += 1
                    bot_response = conversational_response(query, session, lang, question)
                    update_session(session)
                    logging.info(f"Returning response: {{'nextQuestion': '{bot_response}', 'state': 'collecting_necessary', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'nextQuestion': bot_response,
                            'state': 'collecting_necessary',
                            'sessionId': session_id,
                            'lang': lang
                        }),
                        'headers': {'Access-Control-Allow-Origin': '*'}
                    }
            elif question == NECESSARY_QUESTIONS[2]:
                if validate_category(query):
                    session['invalid_attempts'][question] = 0
                    session['necessary_answers'][question] = query
                else:
                    session['invalid_attempts'][question] += 1
                    bot_response = conversational_response(query, session, lang, question)
                    update_session(session)
                    logging.info(f"Returning response: {{'nextQuestion': '{bot_response}', 'state': 'collecting_necessary', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'nextQuestion': bot_response,
                            'state': 'collecting_necessary',
                            'sessionId': session_id,
                            'lang': lang
                        }),
                        'headers': {'Access-Control-Allow-Origin': '*'}
                    }

        if current_index < len(NECESSARY_QUESTIONS):
            if current_index == 2 and 'category' in session:
                session['necessary_answers'][NECESSARY_QUESTIONS[current_index]] = session['category']
                current_index += 1
                session['current_question_index'] = current_index
                update_session(session)
            if current_index < len(NECESSARY_QUESTIONS):
                next_question = conversational_response("", session, lang, NECESSARY_QUESTIONS[current_index])
                update_session(session)
                session['current_question_index'] = current_index + 1
                logging.info(f"Returning response: {{'nextQuestion': '{next_question}', 'state': 'collecting_necessary', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'nextQuestion': next_question,
                        'state': 'collecting_necessary',
                        'sessionId': session_id,
                        'lang': lang
                    }),
                    'headers': {'Access-Control-Allow-Origin': '*'}
                }
        else:
            session['state'] = 'asking_optional'
            session['current_question_index'] = 0
            update_session(session)

    if session['state'] == 'asking_optional':
        current_index = int(session['current_question_index'])
        if current_index > 0:
            question = session['current_optional_question']
            session['optional_answers'][question] = query

        if 'total_optional_questions' not in session:
            session['total_optional_questions'] = 2  # Fixed to 2 for simplicity

        if 'category' in session:
            optional_questions = CATEGORY_OPTIONAL_QUESTIONS.get(session['category'], [])
        else:
            optional_questions = []

        if current_index < session['total_optional_questions']:
            remaining_questions = [q for q in optional_questions if q not in session['optional_answers'].keys()]
            if not remaining_questions:
                session['state'] = 'delivering_fortune'
                session['current_question_index'] = 0
                update_session(session)
            else:
                next_question = conversational_response("", session, lang, remaining_questions[0])
                session['current_optional_question'] = remaining_questions[0]
                session['current_question_index'] = current_index + 1
                update_session(session)
                logging.info(f"Returning response: {{'nextQuestion': '{next_question}', 'state': 'asking_optional', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'nextQuestion': next_question,
                        'state': 'asking_optional',
                        'sessionId': session_id,
                        'lang': lang
                    }),
                    'headers': {'Access-Control-Allow-Origin': '*'}
                }
        else:
            session['state'] = 'delivering_fortune'
            session['current_question_index'] = 0
            update_session(session)

    if session['state'] == 'delivering_fortune':
        birth_datetime = session['necessary_answers'].get('What is your birth date and time, seeker? (e.g., 1990-01-01 14:00)', '')
        location = session['necessary_answers'].get('What is your birth location, seeker? (e.g., Beijing, China)', '')
        category = session['necessary_answers'].get('Are you seeking guidance on love, career, or health?', '').lower()
        if 'category' in session:
            category = session['category']

        if not validate_datetime(birth_datetime):
            logging.info(f"Invalid datetime format in delivering_fortune: {birth_datetime}")
            session['state'] = 'collecting_necessary'
            session['current_question_index'] = 0
            update_session(session)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'nextQuestion': "I need your birth date and time to read your fortune, seeker. Could you share it, like 1990-01-01 14:00?",
                    'state': 'collecting_necessary',
                    'sessionId': session_id,
                    'lang': lang
                }),
                'headers': {'Access-Control-Allow-Origin': '*'}
            }
        if not validate_category(category):
            logging.info(f"Invalid category in delivering_fortune: {category}")
            session['state'] = 'collecting_necessary'
            session['current_question_index'] = 2
            update_session(session)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'nextQuestion': "I can guide you on love, career, or health, seeker. Which would you like to explore?",
                    'state': 'collecting_necessary',
                    'sessionId': session_id,
                    'lang': lang
                }),
                'headers': {'Access-Control-Allow-Origin': '*'}
            }

        chart_gen_start_time = time.time()
        logging.info(f"Generating BaZi chart with birth_datetime: {birth_datetime}, location: {location}, category: {category}")
        try:
            four_pillars = get_four_pillars(birth_datetime, location)
            logging.info(f"BaZi chart: {four_pillars}")
            luck_pillars = get_luck_pillars(four_pillars, gender='unknown')
            logging.info(f"Luck pillars: {luck_pillars}")
            five_elements = get_five_elements(four_pillars)
            logging.info(f"Five Elements: {five_elements}")
        except Exception as e:
            logging.error(f"Error generating BaZi chart: {str(e)}")
            raise Exception("Failed to generate BaZi chart due to an internal error.")
        logging.info(f"BaZi chart generation took {time.time() - chart_gen_start_time:.2f} seconds")

        chart = {
            'four_pillars': four_pillars,
            'luck_pillars': luck_pillars,
            'five_elements': five_elements,
            'category': category
        }

        # Prepare context for LLM
        context = {
            "four_pillars": four_pillars,
            "five_elements": five_elements,
            "user_query": query,
            "lang": lang
        }

        fortune = generate_fortune(chart, context)

        session['state'] = 'collecting_necessary'
        session['necessary_answers'] = {}
        session['optional_answers'] = {}
        session['current_question_index'] = 0
        if 'total_optional_questions' in session:
            del session['total_optional_questions']
        if 'invalid_attempts' in session:
            del session['invalid_attempts']
        if 'conversation_history' in session:
            session['conversation_history'] = []
        if 'category' in session:
            del session['category']
        update_session(session)

        logging.info(f"Returning response: {{'response': '{fortune}', 'state': 'delivered', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
        logging.info(f"lambda_handler took {time.time() - start_time:.2f} seconds")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'response': fortune,
                'state': 'delivered',
                'sessionId': session_id,
                'lang': lang
            }),
            'headers': {'Access-Control-Allow-Origin': '*'}
        }

    # Fallback: If the state is unknown, start collecting necessary information
    bot_response = conversational_response("", session, lang, NECESSARY_QUESTIONS[0])
    session['state'] = 'collecting_necessary'
    session['current_question_index'] = 1
    if 'conversation_history' in session:
        session['conversation_history'] = []
    update_session(session)
    logging.info(f"Returning response: {{'nextQuestion': '{bot_response}', 'state': 'collecting_necessary', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
    logging.info(f"lambda_handler took {time.time() - start_time:.2f} seconds")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'nextQuestion': bot_response,
            'state': 'collecting_necessary',
            'sessionId': session_id,
            'lang': lang
        }),
        'headers': {'Access-Control-Allow-Origin': '*'}
    }
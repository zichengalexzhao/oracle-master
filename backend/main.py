import json
import time
import uuid
import random
import logging
import re
from datetime import datetime
import boto3
import botocore.exceptions

# 初始化 AWS 客户端
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('ZhouyiSessions')
bedrock = boto3.client('bedrock-agent-runtime', region_name='us-east-2')
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-2')

# Knowledge Base ID
KNOWLEDGE_BASE_ID = "EJOOLEA0PL"

# 必要问题列表
NECESSARY_QUESTIONS = ["birth_datetime", "location", "category"]

# 设置日志
logging.basicConfig(level=logging.INFO)

def detect_language(text):
    if any('\u4e00' <= char <= '\u9fff' for char in text):
        return 'zh'
    return 'en'

def extract_datetime(text):
    date_time_pattern = r'\b(\d{4})-(\d{1,2})-(\d{1,2})(?:\s+(\d{1,2}):(\d{2}))?\b'
    date_pattern_zh = r'\b(\d{4})年(\d{1,2})月(\d{1,2})日?\b'
    date_pattern_zh_alt = r'\b(\d{4})年(\d{1,2})月(\d{1,2})号\b'
    date_pattern_en = r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b'
    date_pattern_en_alt = r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?\s*,\s+\d{4}\b'
    date_pattern_en_alt2 = r'\b\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b'
    time_pattern = r'(?:at\s+(\d{1,2})(?::(\d{2}))?\s*(?:AM|PM|am|pm)|(?:上午|下午)(\d{1,2})(?::(\d{2}))?点?)'

    match = re.search(date_time_pattern, text)
    if match:
        date_str = f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        if match.group(4) and match.group(5):
            time_str = f"{int(match.group(4)):02d}:{int(match.group(5)):02d}"
            logging.info(f"Extracted datetime (pattern 1): {date_str} {time_str}")
            return f"{date_str} {time_str}"
        logging.info(f"Extracted datetime (pattern 1, no time): {date_str} 00:00")
        return f"{date_str} 00:00"

    match = re.search(date_pattern_zh, text)
    if not match:
        match = re.search(date_pattern_zh_alt, text)
    if match:
        date_str = f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        time_match = re.search(time_pattern, text)
        if time_match:
            if time_match.group(1):
                hour = int(time_match.group(1))
                minute = int(time_match.group(2) or 0)
                if 'PM' in time_match.group(0) or 'pm' in time_match.group(0):
                    hour = (hour % 12) + 12 if hour != 12 else 12
                elif 'AM' in time_match.group(0) or 'am' in time_match.group(0):
                    hour = hour % 12
                time_str = f"{hour:02d}:{minute:02d}"
                logging.info(f"Extracted datetime (zh pattern): {date_str} {time_str}")
                return f"{date_str} {time_str}"
            elif time_match.group(3):
                hour = int(time_match.group(3))
                minute = int(time_match.group(4) or 0)
                if '下午' in time_match.group(0):
                    hour = (hour % 12) + 12 if hour != 12 else 12
                time_str = f"{hour:02d}:{minute:02d}"
                logging.info(f"Extracted datetime (zh pattern): {date_str} {time_str}")
                return f"{date_str} {time_str}"
        logging.info(f"Extracted datetime (zh pattern, no time): {date_str} 00:00")
        return f"{date_str} 00:00"

    match = re.search(date_pattern_en, text)
    if not match:
        match = re.search(date_pattern_en_alt, text)
    if not match:
        match = re.search(date_pattern_en_alt2, text)
    if match:
        try:
            date_str = match.group(0).strip()
            date_str = re.sub(r'\s+', ' ', date_str)
            if 'st' in date_str or 'nd' in date_str or 'rd' in date_str or 'th' in date_str:
                date_str = re.sub(r'(st|nd|rd|th)', '', date_str).strip()
            if match.re == date_pattern_en_alt:
                date_obj = datetime.strptime(date_str, "%B %d, %Y")
            else:
                date_obj = datetime.strptime(date_str, "%d %B %Y")
            formatted_date = date_obj.strftime("%Y-%m-%d")
            time_match = re.search(time_pattern, text)
            if time_match:
                if time_match.group(1):
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2) or 0)
                    if 'PM' in time_match.group(0) or 'pm' in time_match.group(0):
                        hour = (hour % 12) + 12 if hour != 12 else 12
                    elif 'AM' in time_match.group(0) or 'am' in time_match.group(0):
                        hour = hour % 12
                    time_str = f"{hour:02d}:{minute:02d}"
                    logging.info(f"Extracted datetime (en pattern): {formatted_date} {time_str}")
                    return f"{formatted_date} {time_str}"
                elif time_match.group(3):
                    hour = int(time_match.group(3))
                    minute = int(time_match.group(4) or 0)
                    if '下午' in time_match.group(0):
                        hour = (hour % 12) + 12 if hour != 12 else 12
                    time_str = f"{hour:02d}:{minute:02d}"
                    logging.info(f"Extracted datetime (en pattern): {formatted_date} {time_str}")
                    return f"{formatted_date} {time_str}"
            logging.info(f"Extracted datetime (en pattern, no time): {formatted_date} 00:00")
            return f"{formatted_date} 00:00"
        except ValueError as e:
            logging.error(f"Failed to parse date: {date_str}, error: {str(e)}")

    prompt = f"""
    Extract the date and time from the following text and format it as YYYY-MM-DD HH:MM.
    If the time is not specified, use 00:00 as the default.
    If the date cannot be extracted, return None.
    Text: "{text}"
    """
    messages = [{"role": "user", "content": prompt}]
    try:
        response = bedrock_runtime.invoke_model(
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": messages,
                "max_tokens": 50,
                "temperature": 0.5,
                "top_p": 0.9
            }),
            modelId="us.anthropic.claude-3-haiku-20240307-v1:0",
            accept="application/json",
            contentType="application/json"
        )
        response_body = json.loads(response.get("body").read())
        extracted_datetime = response_body.get("content", [{}])[0].get("text", "")
        if extracted_datetime.lower() == "none":
            logging.info("LLM extracted datetime: None")
            return None
        try:
            datetime.strptime(extracted_datetime, "%Y-%m-%d %H:%M")
            logging.info(f"LLM extracted datetime: {extracted_datetime}")
            return extracted_datetime
        except ValueError:
            logging.error(f"Invalid datetime format returned by LLM: {extracted_datetime}")
            return None
    except Exception as e:
        logging.error(f"Error extracting datetime with Claude 3 Haiku: {str(e)}")
        return None

def extract_location(text):
    location_pattern = r'(?:born\s+in|出生于|出生在)\s*([A-Za-z\s,]+(?:Province|City)?)\b'
    location_pattern_zh = r'(?:出生于|出生在)\s*([\u4e00-\u9fff\s,]+(?:省|市)?)\b'
    
    match = re.search(location_pattern, text, re.IGNORECASE)
    if match:
        location = match.group(1).strip()
        logging.info(f"Extracted location (pattern 1): {location}")
        return location
    
    match = re.search(location_pattern_zh, text)
    if match:
        location = match.group(1).strip()
        logging.info(f"Extracted location (zh pattern): {location}")
        return location
    
    if ',' in text:
        parts = text.split(',')
        for part in parts:
            part = part.strip()
            if part and any(c.isalpha() for c in part):
                logging.info(f"Extracted location (comma split): {part}")
                return part
    
    city_pattern = r'\b(?:[A-Za-z]+(?:\s+[A-Za-z]+)?)\b'
    words = text.split()
    for word in words:
        if re.match(city_pattern, word) and word not in ['born', 'in', 'at', 'on', 'the']:
            logging.info(f"Extracted location (city pattern): {word}")
            return word
    
    prompt = f"""
    Extract the location (city or region) from the following text. If no specific location is mentioned, return None.
    Text: "{text}"
    """
    messages = [{"role": "user", "content": prompt}]
    try:
        response = invoke_bedrock_with_retry(messages)
        response_body = json.loads(response.get("body").read())
        location = response_body.get("content", [{}])[0].get("text", "")
        if location and location.lower() != "none":
            logging.info(f"LLM extracted location: {location}")
            return location
        else:
            logging.info("LLM extracted location: None")
            return None
    except Exception as e:
        logging.error(f"Error extracting location with Bedrock: {str(e)}")
        return None

def extract_category(text, lang):
    love_pattern_en = r'\b(love|relationship|marriage|partner|boyfriend|girlfriend|spouse|romance|dating)\b'
    career_pattern_en = r'\b(career|job|work|employment|business|promotion|prospects|future\s+job|destiny\s+.*?\s+career|professional)\b'
    health_pattern_en = r'\b(health|wellbeing|well-being|illness|disease|fitness|wellness)\b'
    
    love_pattern_zh = r'\b(爱情|感情|恋爱|婚姻|伴侣|男朋友|女朋友|配偶)\b'
    career_pattern_zh = r'\b(事业|工作|就业|生意|晋升|前途|职业)\b'
    health_pattern_zh = r'\b(健康|身体|疾病|养生)\b'
    
    text_lower = text.lower() if lang == 'en' else text
    
    if lang == 'en':
        if re.search(love_pattern_en, text_lower, re.IGNORECASE):
            logging.info("Extracted category: love")
            return 'love'
        if re.search(career_pattern_en, text_lower, re.IGNORECASE):
            logging.info("Extracted category: career")
            return 'career'
        if re.search(health_pattern_en, text_lower, re.IGNORECASE):
            logging.info("Extracted category: health")
            return 'health'
    else:
        if re.search(love_pattern_zh, text):
            logging.info("Extracted category: love")
            return 'love'
        if re.search(career_pattern_zh, text):
            logging.info("Extracted category: career")
            return 'career'
        if re.search(health_pattern_zh, text):
            logging.info("Extracted category: health")
            return 'health'
    
    prompt = f"""
    Determine the category of the user's query from the following options: love, career, health.
    If the query does not match any category, return None.
    Text: "{text}"
    Language: {"English" if lang == 'en' else "Chinese"}
    """
    messages = [{"role": "user", "content": prompt}]
    try:
        response = invoke_bedrock_with_retry(messages)
        response_body = json.loads(response.get("body").read())
        category = response_body.get("content", [{}])[0].get("text", "")
        if category and category.lower() != "none":
            logging.info(f"LLM extracted category: {category}")
            return category
        else:
            logging.info("LLM extracted category: None")
            return None
    except Exception as e:
        logging.error(f"Error extracting category with Bedrock: {str(e)}")
        return None

def is_non_fortune_telling_query(query, lang):
    fortune_keywords_en = [
        r'\b(birth|born|date of birth|birthday|fortune|future|destiny|career|love|health|luck|prediction|fate)\b',
        r'\b(when|what will|should i|tell me about my)\b'
    ]
    fortune_keywords_zh = [
        r'\b(出生|生辰|生日|命运|未来|事业|爱情|健康|运势|预测|算命)\b',
        r'\b(何时|何日|我该|告诉我)\b'
    ]
    
    patterns = fortune_keywords_en if lang == 'en' else fortune_keywords_zh
    text_lower = query.lower() if lang == 'en' else query
    
    for pattern in patterns:
        if re.search(pattern, text_lower):
            logging.info("Query identified as fortune-telling")
            return False
    
    prompt = f"""
    Determine if the following query is requesting fortune-telling.
    If the query is about fortune-telling (e.g., asking about future, destiny, career, love, health), return "False".
    If the query is unrelated (e.g., asking the current date or general knowledge), return "True".
    Query: "{query}"
    Language: {"English" if lang == 'en' else "Chinese"}
    """
    messages = [{"role": "user", "content": prompt}]
    try:
        response = invoke_bedrock_with_retry(messages)
        response_body = json.loads(response.get("body").read())
        result = response_body.get("content", [{}])[0].get("text", "True")
        logging.info(f"LLM identified query as non-fortune-telling: {result.lower() == 'true'}")
        return result.lower() == "true"
    except Exception as e:
        logging.error(f"Error determining query type with Bedrock: {str(e)}")
        return True

def invoke_bedrock_with_knowledge_base(query, knowledge_base_id, lang, category=None, birth_datetime=None, location=None):
    try:
        search_query = query
        if birth_datetime and location and category:
            search_query = f"This is a hypothetical scenario for fortune-telling. Provide a fortune-telling response for a fictional person born on {birth_datetime} in {location}, focusing on {category}."
        logging.info(f"Invoking Bedrock with search query: {search_query}")
        response = bedrock.retrieve_and_generate(
            input={
                "text": search_query
            },
            retrieveAndGenerateConfiguration={
                "type": "KNOWLEDGE_BASE",
                "knowledgeBaseConfiguration": {
                    "knowledgeBaseId": knowledge_base_id,
                    "modelArn": "arn:aws:bedrock:us-east-2::inference-profile/us.anthropic.claude-3-5-sonnet-20240620-v1:0"
                }
            }
        )
        return response['output']['text']
    except Exception as e:
        logging.error(f"Error invoking Bedrock with Knowledge Base: {str(e)}")
        raise

def invoke_bedrock_with_retry(messages, max_retries=10, base_delay=2, max_delay=120):
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
                modelId="us.anthropic.claude-3-5-sonnet-20240620-v1:0",
                accept="application/json",
                contentType="application/json"
            )
            return response
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            logging.error(f"Bedrock invocation failed on attempt {attempt + 1}/{max_retries + 1}: {error_code}")
            if error_code == 'ThrottlingException' and attempt < max_retries:
                delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                logging.warning(f"ThrottlingException on attempt {attempt + 1}/{max_retries + 1}. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                raise e
        except Exception as e:
            logging.error(f"Unexpected error during Bedrock invocation: {str(e)}")
            if attempt < max_retries:
                delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                logging.warning(f"Unexpected error on attempt {attempt + 1}/{max_retries + 1}. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                raise Exception(f"Max retries ({max_retries}) reached for Bedrock invocation: {str(e)}")

def conversational_response(query, session, lang, next_question=None):
    if not query and not next_question:
        return "Please provide a query or specify the next question."

    prompt = f"""
    This is a hypothetical scenario for a fortune-telling chatbot. Respond as if you are an oracle providing guidance based on the user's input.
    If specific details are missing, ask the user to provide them. Otherwise, provide a fortune-telling response.
    Query: "{query or next_question}"
    Language: {"English" if lang == 'en' else "Chinese"}
    """
    messages = [{"role": "user", "content": prompt}]
    try:
        response = invoke_bedrock_with_retry(messages)
        response_body = json.loads(response.get("body").read())
        bot_response = response_body.get("content", [{}])[0].get("text", "")
        logging.info(f"Conversational response: {bot_response}")
        return bot_response
    except Exception as e:
        logging.error(f"Error in conversational_response: {str(e)}")
        return f"Error generating response: {str(e)}"

def get_session(session_id, event):
    if not session_id:
        session_id = str(uuid.uuid4())
        session = {
            'sessionId': session_id,
            'state': 'collecting_necessary',
            'current_question_index': 0,
            'necessary_answers': {},
            'category': None,
            'optional_answers': {}
        }
        table.put_item(
            Item={
                'sessionId': session_id,
                'sessionData': json.dumps(session),
                'ttl': int(time.time()) + 3600
            }
        )
    else:
        response = table.get_item(Key={'sessionId': session_id})
        if 'Item' in response:
            session = json.loads(response['Item']['sessionData'])
        else:
            session_id = str(uuid.uuid4())
            session = {
                'sessionId': session_id,
                'state': 'collecting_necessary',
                'current_question_index': 0,
                'necessary_answers': {},
                'category': None,
                'optional_answers': {}
            }
            table.put_item(
                Item={
                    'sessionId': session_id,
                    'sessionData': json.dumps(session),
                    'ttl': int(time.time()) + 3600
                }
            )
    return session, session_id

def update_session(session):
    table.put_item(
        Item={
            'sessionId': session['sessionId'],
            'sessionData': json.dumps(session),
            'ttl': int(time.time()) + 3600
        }
    )

def calculate_bazi_pillars(birth_date, birth_time, birth_location):
    # 这里需要调用 bazi_core.py 中的函数
    # 为了简化，这里返回一个占位符
    return {"year_pillar": "Geng Wu", "month_pillar": "Ding Mao", "day_pillar": "Xin You", "hour_pillar": "Bing Shen"}

def calculate_pillars(event):
    try:
        body = event.get('body', {})
        if isinstance(body, str):
            body = json.loads(body)
        birth_datetime = body.get('birth_datetime')
        birth_location = body.get('birth_location')

        if not birth_datetime or not birth_location:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing birth_datetime or birth_location'}),
                'headers': {'Access-Control-Allow-Origin': '*'}
            }

        birth_datetime_obj = datetime.strptime(birth_datetime, "%Y-%m-%d %H:%M")
        birth_date = birth_datetime_obj.date()
        birth_time = birth_datetime_obj.time()
        birth_location = birth_location

        pillars = calculate_bazi_pillars(birth_date, birth_time, birth_location)
        return {
            'statusCode': 200,
            'body': json.dumps(pillars),
            'headers': {'Access-Control-Allow-Origin': '*'}
        }
    except Exception as e:
        logging.error(f"Error in calculate_pillars: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {'Access-Control-Allow-Origin': '*'}
        }

def lambda_handler(event, context):
    start_time = time.time()
    logging.info(f"Full event: {json.dumps(event)}")

    path = event.get('path', '').strip()

    if path == '/calculate-pillars':
        return calculate_pillars(event)

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

    # 提取出生信息
    birth_datetime = extract_datetime(query)
    location = extract_location(query)
    category = extract_category(query, lang)

    # 更新 session 中的必要信息
    if birth_datetime:
        session['necessary_answers'][NECESSARY_QUESTIONS[0]] = birth_datetime
    if location:
        session['necessary_answers'][NECESSARY_QUESTIONS[1]] = location
    if category:
        session['necessary_answers'][NECESSARY_QUESTIONS[2]] = category

    if birth_datetime and location and category:
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
        extracted_category = extract_category(query, lang)
        if extracted_category:
            session['category'] = extracted_category

    if session['state'] == 'collecting_necessary':
        current_index = int(session['current_question_index'])
        necessary_answers = session['necessary_answers']
        missing_info = []
        if NECESSARY_QUESTIONS[0] not in necessary_answers:
            missing_info.append("birth date and time (e.g., 1990-01-01 14:00)")
        if NECESSARY_QUESTIONS[1] not in necessary_answers:
            missing_info.append("birth location (e.g., Beijing, China)")
        if NECESSARY_QUESTIONS[2] not in necessary_answers:
            missing_info.append("category (love, career, or health)")

        if not missing_info:
            session['state'] = 'delivering_fortune'
            session['current_question_index'] = 0
            update_session(session)
        else:
            if current_index < len(NECESSARY_QUESTIONS):
                if current_index == 2 and 'category' in session:
                    session['necessary_answers'][NECESSARY_QUESTIONS[current_index]] = session['category']
                    current_index += 1
                    session['current_question_index'] = current_index
                    update_session(session)
                if current_index < len(NECESSARY_QUESTIONS):
                    missing_info_str = ", ".join(missing_info)
                    if lang == 'en':
                        prompt = f"""
                        I need the following information to proceed with fortune-telling: {missing_info_str}. Please provide the missing details.
                        Providing the exact time of birth will lead to a more accurate reading.
                        """
                    else:
                        prompt = f"""
                        贫道需知以下信息以推演您的命运：{missing_info_str}。请提供缺失的细节。
                        提供确切的出生时间将使预测更加准确。
                        """
                    next_question = conversational_response(prompt, session, lang)
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

    if session['state'] == 'delivering_fortune':
        necessary_answers = session['necessary_answers']
        birth_datetime = necessary_answers.get(NECESSARY_QUESTIONS[0])
        location = necessary_answers.get(NECESSARY_QUESTIONS[1])
        category = necessary_answers.get(NECESSARY_QUESTIONS[2])

        try:
            birth_datetime_obj = datetime.strptime(birth_datetime, "%Y-%m-%d %H:%M")
            birth_date = birth_datetime_obj.date()
            birth_time = birth_datetime_obj.time()
            birth_location = location
        except ValueError as e:
            logging.error(f"Invalid birth datetime format: {birth_datetime}, error: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps({'response': "Error: Invalid birth date and time format. Please use YYYY-MM-DD HH:MM.", 'state': 'collecting_necessary'}),
                'headers': {'Access-Control-Allow-Origin': '*'}
            }

        try:
            pillars = calculate_bazi_pillars(birth_date, birth_time, birth_location)
        except Exception as e:
            logging.error(f"Error calculating BaZi pillars: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'response': f"Error calculating BaZi pillars: {str(e)}", 'state': 'delivered'}),
                'headers': {'Access-Control-Allow-Origin': '*'}
            }

        fortune_query = f"Provide a fortune-telling response for a person born on {birth_datetime} in {birth_location}, focusing on {category}."
        fortune_response = invoke_bedrock_with_knowledge_base(fortune_query, KNOWLEDGE_BASE_ID, lang, category, birth_datetime, birth_location)

        session['state'] = 'delivered'
        update_session(session)

        logging.info(f"Returning fortune: {{'response': '{fortune_response}', 'state': 'delivered', 'sessionId': '{session_id}', 'lang': '{lang}'}}")
        logging.info(f"lambda_handler took {time.time() - start_time:.2f} seconds")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'response': fortune_response,
                'state': 'delivered',
                'sessionId': session_id,
                'lang': lang
            }),
            'headers': {'Access-Control-Allow-Origin': '*'}
        }
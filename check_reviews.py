import json
import os
import requests
import time
from datetime import datetime, timedelta

# Configuration from environment variables
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
BRIGHT_DATA_API_KEY = os.environ.get('BRIGHT_DATA_API_KEY')
BRIGHT_DATA_ENDPOINT = os.environ.get('BRIGHT_DATA_ENDPOINT')
STATE_FILE = 'last_review.json'

def validate_secrets():
    """Validate all required secrets are present"""
    missing = []
    
    if not SLACK_WEBHOOK_URL:
        missing.append("SLACK_WEBHOOK_URL")
    if not BRIGHT_DATA_API_KEY:
        missing.append("BRIGHT_DATA_API_KEY")
    if not BRIGHT_DATA_ENDPOINT:
        missing.append("BRIGHT_DATA_ENDPOINT")
    
    if missing:
        error_msg = f"Missing required secrets: {', '.join(missing)}"
        print(f"❌ {error_msg}")
        return False
    
    print("✅ All secrets validated")
    return True

def load_last_review_id():
    """Load the last review ID and list of seen IDs from file"""
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('last_review_id', 0), data.get('seen_review_ids', [])
    except FileNotFoundError:
        return 0, []

def save_last_review_id(review_id, seen_ids):
    """Save the last review ID and seen IDs to file"""
    # Keep only the last 100 seen IDs to prevent file from growing too large
    seen_ids = list(set(seen_ids))[-100:]
    
    with open(STATE_FILE, 'w') as f:
        json.dump({
            'last_review_id': review_id,
            'seen_review_ids': seen_ids,
            'last_checked': datetime.utcnow().isoformat(),
            'last_notification_sent': datetime.utcnow().isoformat()
        }, f, indent=2)

def get_last_notification_time():
    """Get the last time a notification was sent"""
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            last_notif = data.get('last_notification_sent')
            if last_notif:
                return datetime.fromisoformat(last_notif)
    except:
        pass
    return None

def should_run_check():
    """Check if enough time has passed since last check"""
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            last_checked = data.get('last_checked')
            if last_checked:
                last_time = datetime.fromisoformat(last_checked)
                now = datetime.utcnow()
                hours_since = (now - last_time).total_seconds() / 3600
                
                if hours_since < 0.5:
                    print(f"⏭️ Skipping - last checked {hours_since:.1f} hours ago (too recent)")
                    return False
    except:
        pass
    
    return True

def is_review_recent(review, days=60):
    """Check if review is from the last X days (or slightly in the future for clock skew)"""
    try:
        review_date = datetime.strptime(review['date'], '%Y-%m-%d')
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        future_cutoff = datetime.utcnow() + timedelta(days=365)  # Allow up to 1 year in future (for date typos)
        
        if cutoff_date <= review_date <= future_cutoff:
            return True
        else:
            print(f"   ⚠️ Review date {review['date']} is outside valid range ({cutoff_date.date()} to {future_cutoff.date()})")
            return False
    except Exception as e:
        print(f"   ⚠️ Could not parse date: {e}")
        return True

def trigger_collection_with_retry(headers, payload, max_retries=3):
    """Trigger Bright Data collection with retry logic"""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"🌐 Triggering collection (Attempt {attempt}/{max_retries})...")
            response = requests.post(BRIGHT_DATA_ENDPOINT, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            trigger_result = response.json()
            
            print(f"✅ Collection triggered successfully")
            return trigger_result
            
        except requests.exceptions.Timeout:
            print(f"⏱️ Timeout on attempt {attempt}")
            if attempt < max_retries:
                wait_time = attempt * 10
                print(f"   Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed after {max_retries} timeout attempts")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error on attempt {attempt}: {e}")
            if attempt < max_retries:
                wait_time = attempt * 10
                print(f"   Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed after {max_retries} attempts")
                return None
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return None
    
    return None

def check_progress_with_retry(snapshot_id, headers, max_wait=1500, check_interval=15):
    """Check collection progress with retry logic - allows up to 25 minutes"""
    elapsed = 0
    progress_url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    while elapsed < max_wait:
        print(f"⏳ Checking progress... ({elapsed}s / {max_wait}s max)")
        time.sleep(check_interval)
        elapsed += check_interval
        
        try:
            progress_response = requests.get(progress_url, headers=headers, timeout=30)
            progress_response.raise_for_status()
            progress_data = progress_response.json()
            
            consecutive_failures = 0
            
            status = progress_data.get('status', 'unknown')
            print(f"   Status: {status}")
            
            if status == 'ready':
                print(f"✅ Data is ready! (took {elapsed}s)")
                return True
            elif status == 'running' or status == 'starting':
                print(f"   Still gathering data...")
                continue
            elif status == 'failed':
                print(f"❌ Collection failed: {progress_data}")
                return False
                
        except requests.exceptions.RequestException as e:
            consecutive_failures += 1
            print(f"⚠️ Error checking progress (failure {consecutive_failures}/{max_consecutive_failures}): {e}")
            
            if consecutive_failures >= max_consecutive_failures:
                print(f"❌ Too many consecutive failures checking progress")
                return False
            
            continue
    
    print(f"❌ Timeout waiting for data (waited {max_wait}s / {max_wait/60:.1f} minutes)")
    return False

def download_data_with_retry(snapshot_id, headers, max_retries=3):
    """Download data with retry logic"""
    download_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json"
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"📥 Downloading data (Attempt {attempt}/{max_retries})...")
            download_response = requests.get(download_url, headers=headers, timeout=60)
            download_response.raise_for_status()
            data = download_response.json()
            
            if data and len(data) > 0:
                print(f"✅ Successfully received {len(data)} reviews")
                return data
            else:
                print("⚠️ No reviews in response")
                return None
                
        except requests.exceptions.Timeout:
            print(f"⏱️ Download timeout on attempt {attempt}")
            if attempt < max_retries:
                wait_time = attempt * 15
                print(f"   Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed to download after {max_retries} attempts")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Download error on attempt {attempt}: {e}")
            if attempt < max_retries:
                wait_time = attempt * 15
                print(f"   Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed to download after {max_retries} attempts")
                return None
                
        except Exception as e:
            print(f"❌ Unexpected download error: {e}")
            return None
    
    return None

def scrape_g2_reviews():
    """Scrape G2 reviews using Bright Data Datasets API with comprehensive retry logic"""
    headers = {
        'Authorization': f'Bearer {BRIGHT_DATA_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = [
        {
            "url": "https://www.g2.com/products/bright-data/reviews",
            "sort_filter": "Most Recent",
            "pages": 1
        }
    ]
    
    try:
        trigger_result = trigger_collection_with_retry(headers, payload, max_retries=3)
        
        if not trigger_result:
            return None
        
        if isinstance(trigger_result, list) and len(trigger_result) > 0:
            snapshot_id = trigger_result[0].get('snapshot_id')
        else:
            snapshot_id = trigger_result.get('snapshot_id')
        
        if not snapshot_id:
            print("❌ No snapshot_id in response")
            return None
        
        print(f"📸 Snapshot ID: {snapshot_id}")
        
        if not check_progress_with_retry(snapshot_id, headers, max_wait=1500, check_interval=15):
            return None
        
        data = download_data_with_retry(snapshot_id, headers, max_retries=3)
        return data
        
    except Exception as e:
        print(f"❌ Unexpected error in scraping process: {e}")
        import traceback
        traceback.print_exc()
        return None

def send_slack_notification(review, max_retries=3):
    """Send notification to Slack with retry logic"""
    for attempt in range(1, max_retries + 1):
        try:
            if not SLACK_WEBHOOK_URL:
                print("❌ SLACK_WEBHOOK_URL not configured")
                return False
            
            first_text = review.get('text', ['No review text available'])
            if isinstance(first_text, list) and len(first_text) > 0:
                first_text = first_text[0]
            elif not isinstance(first_text, str):
                first_text = "No review text available"
            
            if "Answer:" in first_text:
                first_text = first_text.split("Answer:")[1].strip()
            
            if len(first_text) > 500:
                first_text = first_text[:500] + "..."
            
            stars = review.get('stars', 0)
            try:
                stars_count = int(float(stars))
                stars_emoji = "⭐" * stars_count
            except:
                stars_emoji = "⭐"
            
            payload = {
                "review_title": review.get('title', 'No title'),
                "review_author": review.get('author', 'Unknown'),
                "review_rating": f"{stars}/5 {stars_emoji}",
                "review_date": review.get('date', 'Unknown date'),
                "review_url": review.get('review_url', 'https://www.g2.com/products/bright-data/reviews'),
                "review_text": first_text
            }
            
            print(f"📤 Sending to Slack: {review.get('title', 'Unknown')} (Attempt {attempt}/{max_retries})")
            
            response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
            response.raise_for_status()
            
            print("✅ Notification sent successfully")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error on attempt {attempt}: {e}")
            if attempt < max_retries:
                wait_time = attempt * 5
                print(f"   Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed after {max_retries} attempts")
                return False
        except Exception as e:
            print(f"❌ Unexpected error sending to Slack: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    return False

def send_error_notification(error_message):
    """Send error notification to Slack"""
    try:
        if not SLACK_WEBHOOK_URL:
            return False
        
        payload = {
            "review_title": "⚠️ G2 Review Monitor - Error Alert",
            "review_author": "Automation System",
            "review_rating": "⚠️ Error",
            "review_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
            "review_url": "https://github.com/shemiafniar/g2-review-monitor/actions",
            "review_text": f"The G2 review monitoring script encountered an error:\n\n{error_message}\n\nThis may be a temporary issue. The system will retry on the next scheduled run."
        }
        
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except:
        return False

def send_health_check():
    """Send weekly health check if no notifications sent recently"""
    last_notif_time = get_last_notification_time()
    
    if last_notif_time:
        days_since = (datetime.utcnow() - last_notif_time).total_seconds() / 86400
        
        if days_since >= 7:
            try:
                payload = {
                    "review_title": "✅ G2 Review Monitor - Weekly Health Check",
                    "review_author": "Automation System",
                    "review_rating": "✅ System Active",
                    "review_date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "review_url": "https://www.g2.com/products/bright-data/reviews",
                    "review_text": f"No new reviews detected in the past {int(days_since)} days.\n\nThe monitoring system is running normally and checking twice daily.\n\nLast notification sent: {last_notif_time.strftime('%Y-%m-%d %H:%M UTC')}"
                }
                
                response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
                response.raise_for_status()
                print(f"✅ Weekly health check sent ({int(days_since)} days since last review)")
            except Exception as e:
                print(f"⚠️ Failed to send health check: {e}")

def main():
    print("=" * 60)
    print("🔍 G2 Review Monitor - Starting Check")
    print(f"⏰ Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 60)
    
    if not validate_secrets():
        print("\n❌ Cannot proceed without required secrets")
        return
    
    if not should_run_check():
        print("\n✅ Check skipped to avoid duplicate runs")
        return
    
    try:
        reviews_data = scrape_g2_reviews()
        
        if not reviews_data or len(reviews_data) == 0:
            error_msg = "Failed to retrieve data from Bright Data API after multiple retry attempts. This may be temporary - the system will retry on the next scheduled run."
            print(f"\n⚠️ {error_msg}")
            send_error_notification(error_msg)
            return
        
        if not isinstance(reviews_data, list):
            error_msg = f"Invalid data format received from Bright Data API. Expected list, got {type(reviews_data).__name__}"
            print(f"\n❌ {error_msg}")
            send_error_notification(error_msg)
            return
        
        last_stored_id, seen_ids = load_last_review_id()
        print(f"\n💾 Last stored review ID: {last_stored_id}")
        print(f"📋 Seen review IDs count: {len(seen_ids)}")
        if len(seen_ids) > 0:
            print(f"📝 Last 5 seen IDs: {sorted(seen_ids)[-5:]}")
        print(f"📊 Total reviews fetched: {len(reviews_data)}")
        
        new_reviews = []
        for i, review in enumerate(reviews_data):
            try:
                if not isinstance(review, dict):
                    print(f"⚠️ Skipping invalid review at index {i} (not a dict): {type(review).__name__}")
                    continue
                
                if 'review_id' not in review:
                    print(f"⚠️ Skipping review at index {i} (missing review_id)")
                    continue
                
                if 'date' not in review:
                    print(f"⚠️ Skipping review at index {i} (missing date)")
                    continue
                
                review_id = review['review_id']
                if not isinstance(review_id, (int, float)):
                    print(f"⚠️ Skipping review with invalid ID type: {type(review_id).__name__}")
                    continue
                
                author_name = review.get('author', 'unknown')[:30]
                print(f"🔍 Checking review {review_id} (date: {review.get('date')}, author: {author_name})...")
                
                # Check if we've seen this review before
                if review_id in seen_ids:
                    print(f"   ⏭️ Already processed (ID {review_id} in seen list)")
                    continue
                
                # Check if review is recent
                if not is_review_recent(review):
                    print(f"   ⏭️ Review too old, skipping")
                    continue
                
                print(f"   ✅ New review detected: {review_id}")
                new_reviews.append(review)
                    
            except Exception as e:
                print(f"⚠️ Error processing review at index {i}: {e}")
                continue
        
        if len(new_reviews) == 0:
            print("\n✨ No new reviews - all caught up!")
            
            send_health_check()
            
            if reviews_data:
                try:
                    valid_ids = [r['review_id'] for r in reviews_data if isinstance(r, dict) and 'review_id' in r]
                    if valid_ids:
                        latest_review_id = max(valid_ids)
                        # Add all current review IDs to seen list
                        seen_ids.extend(valid_ids)
                        save_last_review_id(latest_review_id, seen_ids)
                        print(f"💾 State updated - Latest review ID: {latest_review_id}, Total seen: {len(set(seen_ids))}")
                except Exception as e:
                    print(f"⚠️ Could not update last review ID: {e}")
            
            print("\n" + "=" * 60)
            print("✅ Check complete")
            print("=" * 60)
            return
        
        # Sort new reviews by date first, then by ID (oldest first)
        new_reviews.sort(key=lambda x: (x.get('date', ''), x['review_id']))
        
        print(f"\n🆕 FOUND {len(new_reviews)} NEW REVIEW(S)!")
        print("=" * 60)
        
        successful_notifications = 0
        failed_reviews = []
        newly_seen_ids = []
        
        for i, review in enumerate(new_reviews, 1):
            try:
                print(f"\n[{i}/{len(new_reviews)}] Processing Review ID: {review.get('review_id', 'unknown')}")
                print(f"  📝 Title: {review.get('title', 'N/A')}")
                print(f"  👤 Author: {review.get('author', 'N/A')}")
                print(f"  ⭐ Rating: {review.get('stars', 'N/A')}/5")
                print(f"  📅 Date: {review.get('date', 'N/A')}")
                
                required_fields = ['review_id', 'title', 'author', 'stars', 'date', 'review_url']
                missing_fields = [field for field in required_fields if field not in review]
                
                if missing_fields:
                    print(f"  ⚠️ Review missing required fields: {missing_fields}")
                    failed_reviews.append(review.get('review_id', 'unknown'))
                    continue
                
                if send_slack_notification(review):
                    successful_notifications += 1
                    newly_seen_ids.append(review['review_id'])
                    if i < len(new_reviews):
                        time.sleep(2)
                else:
                    failed_reviews.append(review['review_id'])
                    print(f"  ❌ Failed to send notification")
                    
            except Exception as e:
                print(f"  ❌ Error processing review: {e}")
                failed_reviews.append(review.get('review_id', 'unknown'))
                continue
        
        print(f"\n{'=' * 60}")
        print(f"✅ Successfully sent {successful_notifications}/{len(new_reviews)} notifications")
        
        if failed_reviews:
            print(f"⚠️ Failed review IDs: {failed_reviews}")
            error_msg = f"Failed to send {len(failed_reviews)} notification(s) for review IDs: {failed_reviews}"
            send_error_notification(error_msg)
        
        try:
            valid_ids = [r['review_id'] for r in reviews_data if isinstance(r, dict) and 'review_id' in r]
            if valid_ids:
                latest_review_id = max(valid_ids)
                # Add all newly processed IDs and current IDs to seen list
                seen_ids.extend(newly_seen_ids)
                seen_ids.extend(valid_ids)
                save_last_review_id(latest_review_id, seen_ids)
                print(f"💾 State updated - Latest review ID: {latest_review_id}, Total seen: {len(set(seen_ids))}")
        except Exception as e:
            print(f"⚠️ Could not update state: {e}")
        
        print("\n" + "=" * 60)
        print("✅ Check complete")
        print("=" * 60)
        
    except Exception as e:
        error_msg = f"Unexpected error in main execution: {str(e)}"
        print(f"\n❌ {error_msg}")
        send_error_notification(error_msg)
        import traceback
        traceback.print_exc()
        print("\n" + "=" * 60)
        print("❌ Check failed")
        print("=" * 60)

if __name__ == '__main__':
    main()

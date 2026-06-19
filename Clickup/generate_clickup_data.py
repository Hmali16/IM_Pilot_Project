# Generate relational ClickUp CSV data linked to Google Ads campaigns for real-world marketing ops
# Co-authored with CoCo

import csv
import random
import uuid
from datetime import datetime, timedelta
import os

random.seed(42)

# =============================================================================
# CONFIGURATION
# =============================================================================
OUTPUT_DIR = "clickup_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DATE_START = datetime(2024, 1, 1)
DATE_END = datetime(2025, 5, 31)
NUM_DAYS = (DATE_END - DATE_START).days

# =============================================================================
# REFERENCE DATA (linked to Google Ads structure)
# =============================================================================

# Workspaces (teams/departments)
WORKSPACES = [
    {"workspace_id": "ws_001", "workspace_name": "Marketing Operations", "owner_email": "marketing.ops@company.com"},
    {"workspace_id": "ws_002", "workspace_name": "Creative Studio", "owner_email": "creative.lead@company.com"},
    {"workspace_id": "ws_003", "workspace_name": "Performance Marketing", "owner_email": "perf.marketing@company.com"},
    {"workspace_id": "ws_004", "workspace_name": "Product Marketing", "owner_email": "product.mktg@company.com"},
]

# Members
MEMBERS = [
    {"member_id": "mem_001", "username": "sarah.johnson", "email": "sarah.johnson@company.com", "role": "admin", "workspace_id": "ws_001"},
    {"member_id": "mem_002", "username": "mike.chen", "email": "mike.chen@company.com", "role": "admin", "workspace_id": "ws_003"},
    {"member_id": "mem_003", "username": "lisa.patel", "email": "lisa.patel@company.com", "role": "member", "workspace_id": "ws_002"},
    {"member_id": "mem_004", "username": "james.wilson", "email": "james.wilson@company.com", "role": "member", "workspace_id": "ws_003"},
    {"member_id": "mem_005", "username": "emma.davis", "email": "emma.davis@company.com", "role": "member", "workspace_id": "ws_002"},
    {"member_id": "mem_006", "username": "alex.martinez", "email": "alex.martinez@company.com", "role": "admin", "workspace_id": "ws_004"},
    {"member_id": "mem_007", "username": "priya.sharma", "email": "priya.sharma@company.com", "role": "member", "workspace_id": "ws_001"},
    {"member_id": "mem_008", "username": "david.kim", "email": "david.kim@company.com", "role": "member", "workspace_id": "ws_003"},
    {"member_id": "mem_009", "username": "rachel.brown", "email": "rachel.brown@company.com", "role": "member", "workspace_id": "ws_004"},
    {"member_id": "mem_010", "username": "tom.nguyen", "email": "tom.nguyen@company.com", "role": "member", "workspace_id": "ws_002"},
    {"member_id": "mem_011", "username": "nina.garcia", "email": "nina.garcia@company.com", "role": "member", "workspace_id": "ws_001"},
    {"member_id": "mem_012", "username": "chris.lee", "email": "chris.lee@company.com", "role": "member", "workspace_id": "ws_003"},
]

# Spaces (align with Google Ads campaign types)
SPACES = [
    {"space_id": "sp_001", "space_name": "Search Campaigns", "workspace_id": "ws_003", "color": "#4A90D9"},
    {"space_id": "sp_002", "space_name": "Display Campaigns", "workspace_id": "ws_003", "color": "#7B68EE"},
    {"space_id": "sp_003", "space_name": "Video Campaigns", "workspace_id": "ws_002", "color": "#E74C3C"},
    {"space_id": "sp_004", "space_name": "Shopping Campaigns", "workspace_id": "ws_003", "color": "#2ECC71"},
    {"space_id": "sp_005", "space_name": "Brand Awareness", "workspace_id": "ws_004", "color": "#F39C12"},
    {"space_id": "sp_006", "space_name": "Retargeting", "workspace_id": "ws_003", "color": "#9B59B6"},
    {"space_id": "sp_007", "space_name": "Creative Assets", "workspace_id": "ws_002", "color": "#1ABC9C"},
    {"space_id": "sp_008", "space_name": "Landing Pages", "workspace_id": "ws_001", "color": "#E67E22"},
]

# Folders (represent campaign groups / ad groups linkage)
FOLDERS = [
    {"folder_id": "fl_001", "folder_name": "Q1 2024 - Search Lead Gen", "space_id": "sp_001", "campaign_id": "camp_gads_001"},
    {"folder_id": "fl_002", "folder_name": "Q1 2024 - Display Retargeting", "space_id": "sp_002", "campaign_id": "camp_gads_002"},
    {"folder_id": "fl_003", "folder_name": "Q2 2024 - YouTube Pre-roll", "space_id": "sp_003", "campaign_id": "camp_gads_003"},
    {"folder_id": "fl_004", "folder_name": "Q2 2024 - Shopping Feed Optimization", "space_id": "sp_004", "campaign_id": "camp_gads_004"},
    {"folder_id": "fl_005", "folder_name": "H1 2024 - Brand Launch", "space_id": "sp_005", "campaign_id": "camp_gads_005"},
    {"folder_id": "fl_006", "folder_name": "Q3 2024 - Search Expansion", "space_id": "sp_001", "campaign_id": "camp_gads_006"},
    {"folder_id": "fl_007", "folder_name": "Q3 2024 - Retargeting Audiences", "space_id": "sp_006", "campaign_id": "camp_gads_007"},
    {"folder_id": "fl_008", "folder_name": "Q4 2024 - Holiday Display", "space_id": "sp_002", "campaign_id": "camp_gads_008"},
    {"folder_id": "fl_009", "folder_name": "Q4 2024 - Holiday Shopping", "space_id": "sp_004", "campaign_id": "camp_gads_009"},
    {"folder_id": "fl_010", "folder_name": "Q1 2025 - New Year Search", "space_id": "sp_001", "campaign_id": "camp_gads_010"},
    {"folder_id": "fl_011", "folder_name": "Evergreen Creative Library", "space_id": "sp_007", "campaign_id": None},
    {"folder_id": "fl_012", "folder_name": "Landing Page A/B Tests", "space_id": "sp_008", "campaign_id": None},
]

# Google Ads Campaign references (simulated IDs that link to GOOGLE_ADS.BRONZE tables)
GADS_CAMPAIGNS = [
    {"campaign_id": "camp_gads_001", "campaign_name": "Search - Enterprise SaaS", "campaign_type": "SEARCH", "budget_daily_usd": 500},
    {"campaign_id": "camp_gads_002", "campaign_name": "Display - Website Visitors Retarget", "campaign_type": "DISPLAY", "budget_daily_usd": 300},
    {"campaign_id": "camp_gads_003", "campaign_name": "Video - Product Demo Pre-roll", "campaign_type": "VIDEO", "budget_daily_usd": 800},
    {"campaign_id": "camp_gads_004", "campaign_name": "Shopping - Electronics Catalog", "campaign_type": "SHOPPING", "budget_daily_usd": 1200},
    {"campaign_id": "camp_gads_005", "campaign_name": "Display - Brand Awareness Q1", "campaign_type": "DISPLAY", "budget_daily_usd": 600},
    {"campaign_id": "camp_gads_006", "campaign_name": "Search - SMB Expansion Pack", "campaign_type": "SEARCH", "budget_daily_usd": 450},
    {"campaign_id": "camp_gads_007", "campaign_name": "Display - Cart Abandoners", "campaign_type": "DISPLAY", "budget_daily_usd": 350},
    {"campaign_id": "camp_gads_008", "campaign_name": "Display - Holiday Season 2024", "campaign_type": "DISPLAY", "budget_daily_usd": 1500},
    {"campaign_id": "camp_gads_009", "campaign_name": "Shopping - Holiday Gift Guide", "campaign_type": "SHOPPING", "budget_daily_usd": 2000},
    {"campaign_id": "camp_gads_010", "campaign_name": "Search - New Year Promo", "campaign_type": "SEARCH", "budget_daily_usd": 700},
]

# Google Ads Ad Groups (linked to campaigns)
GADS_AD_GROUPS = [
    {"ad_group_id": "ag_001", "ad_group_name": "Enterprise - CRM Keywords", "campaign_id": "camp_gads_001", "status": "ENABLED"},
    {"ad_group_id": "ag_002", "ad_group_name": "Enterprise - ERP Keywords", "campaign_id": "camp_gads_001", "status": "ENABLED"},
    {"ad_group_id": "ag_003", "ad_group_name": "Retarget - Homepage Visitors", "campaign_id": "camp_gads_002", "status": "ENABLED"},
    {"ad_group_id": "ag_004", "ad_group_name": "Retarget - Pricing Page", "campaign_id": "camp_gads_002", "status": "ENABLED"},
    {"ad_group_id": "ag_005", "ad_group_name": "Pre-roll - 15s Bumper", "campaign_id": "camp_gads_003", "status": "ENABLED"},
    {"ad_group_id": "ag_006", "ad_group_name": "Pre-roll - 30s Skippable", "campaign_id": "camp_gads_003", "status": "ENABLED"},
    {"ad_group_id": "ag_007", "ad_group_name": "Electronics - Laptops", "campaign_id": "camp_gads_004", "status": "ENABLED"},
    {"ad_group_id": "ag_008", "ad_group_name": "Electronics - Accessories", "campaign_id": "camp_gads_004", "status": "PAUSED"},
    {"ad_group_id": "ag_009", "ad_group_name": "Brand - Competitor Conquesting", "campaign_id": "camp_gads_005", "status": "ENABLED"},
    {"ad_group_id": "ag_010", "ad_group_name": "SMB - Accounting Software", "campaign_id": "camp_gads_006", "status": "ENABLED"},
    {"ad_group_id": "ag_011", "ad_group_name": "SMB - HR Tools", "campaign_id": "camp_gads_006", "status": "ENABLED"},
    {"ad_group_id": "ag_012", "ad_group_name": "Cart Abandon - High Value", "campaign_id": "camp_gads_007", "status": "ENABLED"},
    {"ad_group_id": "ag_013", "ad_group_name": "Holiday - Gift Sets", "campaign_id": "camp_gads_008", "status": "ENABLED"},
    {"ad_group_id": "ag_014", "ad_group_name": "Holiday Shopping - Under $50", "campaign_id": "camp_gads_009", "status": "ENABLED"},
    {"ad_group_id": "ag_015", "ad_group_name": "Holiday Shopping - Premium", "campaign_id": "camp_gads_009", "status": "ENABLED"},
    {"ad_group_id": "ag_016", "ad_group_name": "New Year - Resolution Deals", "campaign_id": "camp_gads_010", "status": "ENABLED"},
]

# Lists (equivalent to task boards within folders)
LISTS = []
list_templates = [
    ("Ad Copy & Creative", "fl_{:03d}"),
    ("Keyword Research", "fl_{:03d}"),
    ("Performance Reviews", "fl_{:03d}"),
    ("Budget Approvals", "fl_{:03d}"),
    ("A/B Testing", "fl_{:03d}"),
]

list_counter = 1
for folder in FOLDERS:
    for tmpl_name, _ in list_templates[:random.randint(2, 5)]:
        LISTS.append({
            "list_id": f"lst_{list_counter:03d}",
            "list_name": tmpl_name,
            "folder_id": folder["folder_id"],
            "status_options": "Open,In Progress,Review,Approved,Closed",
        })
        list_counter += 1

# Custom Fields (metadata for tasks)
CUSTOM_FIELDS = [
    {"field_id": "cf_001", "field_name": "Google Ads Campaign ID", "field_type": "short_text", "workspace_id": "ws_003"},
    {"field_id": "cf_002", "field_name": "Ad Group ID", "field_type": "short_text", "workspace_id": "ws_003"},
    {"field_id": "cf_003", "field_name": "Target CPA ($)", "field_type": "number", "workspace_id": "ws_003"},
    {"field_id": "cf_004", "field_name": "Target ROAS", "field_type": "number", "workspace_id": "ws_003"},
    {"field_id": "cf_005", "field_name": "Creative Format", "field_type": "dropdown", "workspace_id": "ws_002"},
    {"field_id": "cf_006", "field_name": "Approval Status", "field_type": "dropdown", "workspace_id": "ws_001"},
    {"field_id": "cf_007", "field_name": "Landing Page URL", "field_type": "url", "workspace_id": "ws_001"},
    {"field_id": "cf_008", "field_name": "Keyword Match Type", "field_type": "dropdown", "workspace_id": "ws_003"},
    {"field_id": "cf_009", "field_name": "Budget Allocated ($)", "field_type": "currency", "workspace_id": "ws_003"},
    {"field_id": "cf_010", "field_name": "UTM Campaign Tag", "field_type": "short_text", "workspace_id": "ws_001"},
    {"field_id": "cf_011", "field_name": "Priority Score", "field_type": "number", "workspace_id": "ws_003"},
    {"field_id": "cf_012", "field_name": "Conversion Goal", "field_type": "dropdown", "workspace_id": "ws_003"},
]

# Tags
TAGS = [
    {"tag_id": "tag_001", "tag_name": "high-priority", "color": "#E74C3C"},
    {"tag_id": "tag_002", "tag_name": "search-ads", "color": "#3498DB"},
    {"tag_id": "tag_003", "tag_name": "display-ads", "color": "#9B59B6"},
    {"tag_id": "tag_004", "tag_name": "video-ads", "color": "#E67E22"},
    {"tag_id": "tag_005", "tag_name": "shopping-ads", "color": "#2ECC71"},
    {"tag_id": "tag_006", "tag_name": "needs-review", "color": "#F1C40F"},
    {"tag_id": "tag_007", "tag_name": "approved", "color": "#27AE60"},
    {"tag_id": "tag_008", "tag_name": "blocked", "color": "#C0392B"},
    {"tag_id": "tag_009", "tag_name": "automation", "color": "#1ABC9C"},
    {"tag_id": "tag_010", "tag_name": "budget-change", "color": "#8E44AD"},
    {"tag_id": "tag_011", "tag_name": "creative-update", "color": "#D35400"},
    {"tag_id": "tag_012", "tag_name": "landing-page", "color": "#2980B9"},
    {"tag_id": "tag_013", "tag_name": "keyword-expansion", "color": "#16A085"},
    {"tag_id": "tag_014", "tag_name": "negative-keywords", "color": "#7F8C8D"},
    {"tag_id": "tag_015", "tag_name": "urgent", "color": "#E74C3C"},
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def random_date(start=DATE_START, end=DATE_END):
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days, hours=random.randint(0, 23), minutes=random.randint(0, 59))

def random_date_after(dt, max_days=30):
    return dt + timedelta(days=random.randint(1, max_days), hours=random.randint(0, 23), minutes=random.randint(0, 59))

def fmt_dt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def generate_task_name(list_name, campaign_type=None):
    search_tasks = [
        "Research new keyword opportunities for {kw}",
        "Add negative keywords for {kw} campaign",
        "Update ad copy: headline variation test",
        "Review search term report - {period}",
        "Adjust bids for top-performing keywords",
        "Create RSA with new headline combinations",
        "Optimize quality scores for {kw} ad group",
        "Pause underperforming keywords (CPA > target)",
        "Expand keyword list: competitor brand terms",
        "Set up automated bidding rules",
    ]
    display_tasks = [
        "Design new banner set: {size}",
        "Create responsive display ad variations",
        "Update audience segments for retargeting",
        "Review placement exclusion list",
        "Design holiday-themed creative set",
        "A/B test: image vs animated banner",
        "Update frequency capping settings",
        "Create lookalike audience from converters",
        "Review viewability metrics and optimize",
        "Update brand safety exclusions",
    ]
    video_tasks = [
        "Script: 15s bumper ad for {product}",
        "Edit: 30s product demo cut-down",
        "Storyboard: testimonial video ad",
        "Upload new video creative to Google Ads",
        "Review video completion rates by audience",
        "Create companion banner for in-stream",
        "Optimize targeting: affinity vs in-market",
        "A/B test: CTA overlay placement",
        "Subtitle/caption review for accessibility",
        "Thumbnail optimization test",
    ]
    shopping_tasks = [
        "Fix disapproved products in feed",
        "Optimize product titles for {category}",
        "Update pricing in merchant center",
        "Add supplemental feed: custom labels",
        "Review ROAS by product category",
        "Set up promotion extensions",
        "Audit product images: compliance check",
        "Segment products by margin tier",
        "Create smart shopping campaign draft",
        "Fix GTINs for rejected items",
    ]
    general_tasks = [
        "Weekly performance review - {period}",
        "Budget reallocation proposal",
        "Competitor analysis: {competitor} campaigns",
        "Create monthly report for stakeholders",
        "Update UTM tracking parameters",
        "Audit conversion tracking setup",
        "Meeting: campaign strategy alignment",
        "Document new process: {process}",
        "Review landing page load speeds",
        "Set up new conversion action in Google Ads",
    ]

    kw_samples = ["CRM software", "project management", "HR tools", "accounting", "ERP system", "cloud hosting"]
    size_samples = ["300x250", "728x90", "160x600", "320x50", "970x250"]
    product_samples = ["SaaS platform", "mobile app", "enterprise suite", "starter plan"]
    category_samples = ["laptops", "headphones", "monitors", "keyboards", "cameras"]
    competitor_samples = ["Competitor A", "Competitor B", "Market Leader"]
    process_samples = ["ad approval workflow", "bid management", "audience sync"]
    period_samples = ["Week 1-2", "Week 3-4", "Monthly", "Q1", "Q2", "Q3", "Q4"]

    if "Keyword" in list_name or campaign_type == "SEARCH":
        task = random.choice(search_tasks)
    elif "Creative" in list_name or campaign_type == "DISPLAY":
        task = random.choice(display_tasks)
    elif campaign_type == "VIDEO":
        task = random.choice(video_tasks)
    elif campaign_type == "SHOPPING":
        task = random.choice(shopping_tasks)
    else:
        task = random.choice(general_tasks)

    return task.format(
        kw=random.choice(kw_samples),
        size=random.choice(size_samples),
        product=random.choice(product_samples),
        category=random.choice(category_samples),
        competitor=random.choice(competitor_samples),
        process=random.choice(process_samples),
        period=random.choice(period_samples),
    )


# =============================================================================
# GENERATE TASKS (main entity)
# =============================================================================

TASKS = []
TASK_ASSIGNEES = []
TASK_TAGS = []
TASK_CUSTOM_FIELD_VALUES = []
TASK_DEPENDENCIES = []
TASK_COMMENTS = []
TASK_TIME_ENTRIES = []
TASK_CHECKLISTS = []
TASK_ATTACHMENTS = []

task_counter = 0
statuses = ["Open", "In Progress", "Review", "Approved", "Closed"]
priorities = ["urgent", "high", "normal", "low"]
priority_weights = [5, 20, 50, 25]

for lst in LISTS:
    folder = next(f for f in FOLDERS if f["folder_id"] == lst["folder_id"])
    campaign = next((c for c in GADS_CAMPAIGNS if c["campaign_id"] == folder.get("campaign_id")), None)
    campaign_type = campaign["campaign_type"] if campaign else None

    num_tasks = random.randint(8, 25)
    for _ in range(num_tasks):
        task_counter += 1
        task_id = f"task_{task_counter:05d}"
        created = random_date()
        due = random_date_after(created, max_days=random.randint(7, 45))
        started = random_date_after(created, max_days=5) if random.random() > 0.2 else None
        status = random.choices(statuses, weights=[10, 25, 20, 20, 25])[0]
        closed_at = random_date_after(due, max_days=5) if status == "Closed" else None
        priority = random.choices(priorities, weights=priority_weights)[0]
        time_estimate_hrs = random.choice([0.5, 1, 2, 3, 4, 5, 8, 16, None, None])

        task = {
            "task_id": task_id,
            "task_name": generate_task_name(lst["list_name"], campaign_type),
            "description": f"Task for {folder['folder_name']} | List: {lst['list_name']}",
            "status": status,
            "priority": priority,
            "list_id": lst["list_id"],
            "folder_id": folder["folder_id"],
            "space_id": next(s["space_id"] for s in SPACES if any(f["space_id"] == s["space_id"] for f in FOLDERS if f["folder_id"] == folder["folder_id"])),
            "workspace_id": next(s["workspace_id"] for s in SPACES if any(f["space_id"] == s["space_id"] for f in FOLDERS if f["folder_id"] == folder["folder_id"])),
            "created_at": fmt_dt(created),
            "updated_at": fmt_dt(random_date_after(created, max_days=10)),
            "due_date": fmt_dt(due),
            "start_date": fmt_dt(started) if started else "",
            "closed_at": fmt_dt(closed_at) if closed_at else "",
            "time_estimate_hours": time_estimate_hrs if time_estimate_hrs else "",
            "creator_id": random.choice(MEMBERS)["member_id"],
            "google_ads_campaign_id": campaign["campaign_id"] if campaign else "",
            "google_ads_campaign_name": campaign["campaign_name"] if campaign else "",
        }
        TASKS.append(task)

        # Assignees (1-3 per task)
        num_assignees = random.randint(1, 3)
        assigned_members = random.sample(MEMBERS, num_assignees)
        for mem in assigned_members:
            TASK_ASSIGNEES.append({
                "task_id": task_id,
                "member_id": mem["member_id"],
                "username": mem["username"],
                "assigned_at": fmt_dt(random_date_after(created, max_days=2)),
            })

        # Tags (0-4 per task)
        num_tags = random.randint(0, 4)
        assigned_tags = random.sample(TAGS, num_tags)
        for tag in assigned_tags:
            TASK_TAGS.append({
                "task_id": task_id,
                "tag_id": tag["tag_id"],
                "tag_name": tag["tag_name"],
            })

        # Custom field values
        if campaign:
            TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_001", "field_name": "Google Ads Campaign ID", "value": campaign["campaign_id"]})
            if random.random() > 0.3:
                ag = random.choice([ag for ag in GADS_AD_GROUPS if ag["campaign_id"] == campaign["campaign_id"]])
                TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_002", "field_name": "Ad Group ID", "value": ag["ad_group_id"]})
            if campaign_type == "SEARCH":
                TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_003", "field_name": "Target CPA ($)", "value": str(round(random.uniform(15, 120), 2))})
                TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_008", "field_name": "Keyword Match Type", "value": random.choice(["Broad", "Phrase", "Exact"])})
            if campaign_type == "SHOPPING":
                TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_004", "field_name": "Target ROAS", "value": str(round(random.uniform(2.0, 8.0), 1))})
            TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_009", "field_name": "Budget Allocated ($)", "value": str(round(random.uniform(500, 15000), 2))})
            TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_010", "field_name": "UTM Campaign Tag", "value": campaign["campaign_name"].lower().replace(" ", "_").replace("-", "")})

        if random.random() > 0.5:
            TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_011", "field_name": "Priority Score", "value": str(random.randint(1, 100))})
        if random.random() > 0.6:
            TASK_CUSTOM_FIELD_VALUES.append({"task_id": task_id, "field_id": "cf_012", "field_name": "Conversion Goal", "value": random.choice(["Lead Form", "Purchase", "Sign Up", "Demo Request", "Add to Cart", "Phone Call"])})

        # Comments (0-8 per task)
        num_comments = random.randint(0, 8)
        comment_date = created
        for c_idx in range(num_comments):
            comment_date = random_date_after(comment_date, max_days=5)
            comment_texts = [
                "Updated the ad copy — please review the new headline.",
                "CPA is trending above target. Consider pausing low performers.",
                "Creative assets uploaded to shared drive. Ready for review.",
                f"Bid adjustment applied: +{random.randint(5,30)}% for mobile.",
                "Waiting on approval from client before going live.",
                "Keyword research complete. Found 45 new opportunities.",
                "Landing page updated. New version is live.",
                "ROAS improved 15% after last week's changes.",
                "Need budget increase approval — current allocation exhausted.",
                "Competitor launched similar campaign. Adjusting strategy.",
                "Quality score improved from 5 to 7 after ad copy changes.",
                "Scheduled meeting to discuss Q4 strategy.",
                "A/B test results: Variant B outperforms by 23%.",
                "Feed issues resolved. All products now approved.",
                "Video ad edit v3 is ready for final sign-off.",
            ]
            TASK_COMMENTS.append({
                "comment_id": f"cmt_{task_counter:05d}_{c_idx+1:02d}",
                "task_id": task_id,
                "author_id": random.choice(MEMBERS)["member_id"],
                "comment_text": random.choice(comment_texts),
                "created_at": fmt_dt(comment_date),
                "resolved": random.choice([True, False, False]),
            })

        # Time entries (for tasks that have work logged)
        if status in ["In Progress", "Review", "Approved", "Closed"] and random.random() > 0.3:
            num_entries = random.randint(1, 5)
            entry_date = started if started else random_date_after(created, max_days=3)
            for te_idx in range(num_entries):
                duration_mins = random.choice([15, 30, 45, 60, 90, 120, 180, 240])
                TASK_TIME_ENTRIES.append({
                    "time_entry_id": f"te_{task_counter:05d}_{te_idx+1:02d}",
                    "task_id": task_id,
                    "member_id": random.choice(assigned_members)["member_id"],
                    "duration_minutes": duration_mins,
                    "description": random.choice(["Research", "Implementation", "Review", "Meeting", "Testing", "Documentation", "Analysis"]),
                    "start_time": fmt_dt(entry_date),
                    "end_time": fmt_dt(entry_date + timedelta(minutes=duration_mins)),
                    "billable": random.choice([True, True, False]),
                })
                entry_date = random_date_after(entry_date, max_days=3)

        # Checklists (30% of tasks)
        if random.random() > 0.7:
            checklist_items = random.randint(3, 8)
            checklist_templates = {
                "SEARCH": ["Research keywords", "Write ad headlines", "Write descriptions", "Set bid strategy", "Add negative keywords", "Set targeting", "Review quality score", "Submit for approval"],
                "DISPLAY": ["Design banner", "Create responsive ad", "Set audience", "Exclusion list", "Frequency cap", "Upload creatives", "QA check", "Launch"],
                "VIDEO": ["Write script", "Record/edit video", "Add captions", "Create thumbnail", "Set targeting", "Upload to YouTube", "Link to Ads", "Monitor"],
                "SHOPPING": ["Update feed", "Fix disapprovals", "Set bids", "Add promotions", "Review ROAS", "Segment products", "Optimize titles", "Submit"],
            }
            items = checklist_templates.get(campaign_type, checklist_templates["SEARCH"])[:checklist_items]
            for cl_idx, item in enumerate(items):
                TASK_CHECKLISTS.append({
                    "checklist_id": f"cl_{task_counter:05d}",
                    "task_id": task_id,
                    "checklist_name": "Task Steps",
                    "item_index": cl_idx + 1,
                    "item_text": item,
                    "is_checked": random.choice([True, True, False]) if status in ["Review", "Approved", "Closed"] else random.choice([True, False, False, False]),
                    "assignee_id": random.choice(assigned_members)["member_id"] if random.random() > 0.5 else "",
                })

        # Attachments (20% of tasks)
        if random.random() > 0.8:
            file_types = [
                ("ad_copy_v{}.docx", "application/docx"),
                ("banner_{}.png", "image/png"),
                ("performance_report_{}.xlsx", "application/xlsx"),
                ("video_edit_v{}.mp4", "video/mp4"),
                ("keyword_list_{}.csv", "text/csv"),
                ("landing_page_mockup_{}.pdf", "application/pdf"),
                ("audience_segment_{}.json", "application/json"),
                ("campaign_brief_{}.pdf", "application/pdf"),
            ]
            num_attachments = random.randint(1, 3)
            for att_idx in range(num_attachments):
                file_tmpl, mime = random.choice(file_types)
                TASK_ATTACHMENTS.append({
                    "attachment_id": f"att_{task_counter:05d}_{att_idx+1:02d}",
                    "task_id": task_id,
                    "file_name": file_tmpl.format(random.randint(1, 5)),
                    "file_size_bytes": random.randint(10000, 50000000),
                    "mime_type": mime,
                    "uploaded_by": random.choice(assigned_members)["member_id"],
                    "uploaded_at": fmt_dt(random_date_after(created, max_days=10)),
                })

# Task Dependencies (create realistic chains)
dependency_counter = 0
task_ids = [t["task_id"] for t in TASKS]
for i in range(0, len(TASKS) - 1, 3):
    if random.random() > 0.6 and i + 1 < len(TASKS):
        if TASKS[i]["list_id"] == TASKS[i+1]["list_id"]:
            dependency_counter += 1
            TASK_DEPENDENCIES.append({
                "dependency_id": f"dep_{dependency_counter:04d}",
                "task_id": TASKS[i+1]["task_id"],
                "depends_on_task_id": TASKS[i]["task_id"],
                "dependency_type": "waiting_on",
            })

# =============================================================================
# GOALS (linked to campaign KPIs)
# =============================================================================

GOALS = []
goal_templates = [
    ("Reduce CPA to ${target} for {campaign}", "currency"),
    ("Achieve {target}x ROAS on {campaign}", "number"),
    ("Increase CTR to {target}% across search campaigns", "percentage"),
    ("Launch {target} new ad creatives this quarter", "number"),
    ("Complete all keyword expansion tasks by EOQ", "true/false"),
    ("Reduce wasted spend by {target}% via negatives", "percentage"),
    ("Hit {target} conversions/month on shopping", "number"),
    ("Improve Quality Score avg to {target}", "number"),
]

for g_idx, (tmpl, metric_type) in enumerate(goal_templates):
    campaign = random.choice(GADS_CAMPAIGNS)
    target_vals = {"currency": round(random.uniform(20, 80), 0), "number": random.randint(3, 50), "percentage": round(random.uniform(2, 15), 1), "true/false": 1}
    target = target_vals[metric_type]
    current = round(target * random.uniform(0.4, 1.2), 2)
    GOALS.append({
        "goal_id": f"goal_{g_idx+1:03d}",
        "goal_name": tmpl.format(target=target, campaign=campaign["campaign_name"]),
        "workspace_id": random.choice(WORKSPACES)["workspace_id"],
        "owner_id": random.choice(MEMBERS)["member_id"],
        "target_value": target,
        "current_value": current,
        "metric_type": metric_type,
        "status": "on_track" if current / target >= 0.7 else "at_risk" if current / target >= 0.4 else "off_track",
        "due_date": fmt_dt(random_date(datetime(2024, 6, 1), DATE_END)),
        "google_ads_campaign_id": campaign["campaign_id"],
        "created_at": fmt_dt(random_date(DATE_START, datetime(2024, 3, 1))),
    })

# =============================================================================
# SPRINTS / TIME PERIODS
# =============================================================================

SPRINTS = []
sprint_start = DATE_START
sprint_counter = 0
while sprint_start < DATE_END:
    sprint_counter += 1
    sprint_end = sprint_start + timedelta(days=14)
    SPRINTS.append({
        "sprint_id": f"sprint_{sprint_counter:03d}",
        "sprint_name": f"Sprint {sprint_counter} - {sprint_start.strftime('%b %d')}",
        "start_date": fmt_dt(sprint_start),
        "end_date": fmt_dt(sprint_end),
        "workspace_id": random.choice(WORKSPACES)["workspace_id"],
        "status": "closed" if sprint_end < datetime(2025, 5, 1) else "active" if sprint_start < datetime(2025, 5, 15) else "planned",
    })
    sprint_start = sprint_end + timedelta(days=1)

# Sprint-task mapping
SPRINT_TASKS = []
for task in TASKS:
    if random.random() > 0.4:
        sprint = random.choice(SPRINTS)
        SPRINT_TASKS.append({
            "sprint_id": sprint["sprint_id"],
            "task_id": task["task_id"],
            "story_points": random.choice([1, 2, 3, 5, 8, 13]),
        })

# =============================================================================
# AUTOMATIONS (rules tied to campaign workflows)
# =============================================================================

AUTOMATIONS = [
    {"automation_id": "auto_001", "name": "Move to Review when all subtasks done", "trigger": "subtask_completion", "action": "change_status", "action_value": "Review", "workspace_id": "ws_003", "enabled": True},
    {"automation_id": "auto_002", "name": "Notify team on budget approval", "trigger": "custom_field_change", "action": "send_notification", "action_value": "Budget Approved", "workspace_id": "ws_003", "enabled": True},
    {"automation_id": "auto_003", "name": "Auto-assign creative tasks to design team", "trigger": "task_created", "action": "assign_member", "action_value": "mem_003,mem_005,mem_010", "workspace_id": "ws_002", "enabled": True},
    {"automation_id": "auto_004", "name": "Set due date 7 days from creation for urgent", "trigger": "priority_set_urgent", "action": "set_due_date", "action_value": "+7d", "workspace_id": "ws_003", "enabled": True},
    {"automation_id": "auto_005", "name": "Archive closed tasks after 30 days", "trigger": "status_closed_30d", "action": "archive_task", "action_value": "archive", "workspace_id": "ws_001", "enabled": True},
    {"automation_id": "auto_006", "name": "Create performance review task weekly", "trigger": "recurring_weekly", "action": "create_task", "action_value": "Weekly Performance Review", "workspace_id": "ws_003", "enabled": True},
    {"automation_id": "auto_007", "name": "Alert on overdue campaign tasks", "trigger": "due_date_passed", "action": "send_notification", "action_value": "Task Overdue Alert", "workspace_id": "ws_003", "enabled": True},
    {"automation_id": "auto_008", "name": "Tag high-spend tasks for review", "trigger": "custom_field_threshold", "action": "add_tag", "action_value": "needs-review", "workspace_id": "ws_003", "enabled": True},
]

# =============================================================================
# VIEWS (saved views for campaign management)
# =============================================================================

VIEWS = [
    {"view_id": "view_001", "view_name": "All Active Campaign Tasks", "view_type": "list", "space_id": "sp_001", "filters": "status != Closed", "group_by": "priority"},
    {"view_id": "view_002", "view_name": "Search Campaigns Board", "view_type": "board", "space_id": "sp_001", "filters": "tag = search-ads", "group_by": "status"},
    {"view_id": "view_003", "view_name": "Creative Pipeline", "view_type": "board", "space_id": "sp_007", "filters": "space = Creative Assets", "group_by": "status"},
    {"view_id": "view_004", "view_name": "Overdue Tasks", "view_type": "list", "space_id": "", "filters": "due_date < today AND status != Closed", "group_by": "assignee"},
    {"view_id": "view_005", "view_name": "Campaign Budget Tracker", "view_type": "table", "space_id": "sp_001", "filters": "custom_field:Budget Allocated > 0", "group_by": "folder"},
    {"view_id": "view_006", "view_name": "Sprint Burndown", "view_type": "gantt", "space_id": "", "filters": "sprint = current", "group_by": "list"},
    {"view_id": "view_007", "view_name": "My Tasks - This Week", "view_type": "list", "space_id": "", "filters": "assignee = me AND due_date <= end_of_week", "group_by": "priority"},
    {"view_id": "view_008", "view_name": "Waiting for Approval", "view_type": "list", "space_id": "", "filters": "status = Review", "group_by": "space"},
    {"view_id": "view_009", "view_name": "Time Tracking Report", "view_type": "table", "space_id": "", "filters": "time_logged > 0", "group_by": "member"},
    {"view_id": "view_010", "view_name": "Shopping Campaign Gantt", "view_type": "gantt", "space_id": "sp_004", "filters": "tag = shopping-ads", "group_by": "folder"},
]

# =============================================================================
# WEBHOOKS / INTEGRATIONS
# =============================================================================

INTEGRATIONS = [
    {"integration_id": "int_001", "name": "Google Ads Sync", "type": "google_ads", "workspace_id": "ws_003", "status": "active", "sync_frequency": "hourly", "last_sync": fmt_dt(random_date(datetime(2025, 5, 1), DATE_END))},
    {"integration_id": "int_002", "name": "Slack Notifications", "type": "slack", "workspace_id": "ws_001", "status": "active", "sync_frequency": "realtime", "last_sync": fmt_dt(random_date(datetime(2025, 5, 1), DATE_END))},
    {"integration_id": "int_003", "name": "Google Analytics Import", "type": "google_analytics", "workspace_id": "ws_003", "status": "active", "sync_frequency": "daily", "last_sync": fmt_dt(random_date(datetime(2025, 5, 1), DATE_END))},
    {"integration_id": "int_004", "name": "Figma Design Sync", "type": "figma", "workspace_id": "ws_002", "status": "active", "sync_frequency": "on_change", "last_sync": fmt_dt(random_date(datetime(2025, 5, 1), DATE_END))},
    {"integration_id": "int_005", "name": "GitHub - Landing Pages", "type": "github", "workspace_id": "ws_001", "status": "active", "sync_frequency": "on_push", "last_sync": fmt_dt(random_date(datetime(2025, 5, 1), DATE_END))},
    {"integration_id": "int_006", "name": "Snowflake Data Warehouse", "type": "snowflake", "workspace_id": "ws_003", "status": "active", "sync_frequency": "daily", "last_sync": fmt_dt(random_date(datetime(2025, 5, 1), DATE_END))},
]

# =============================================================================
# AUDIT LOG (activity tracking)
# =============================================================================

AUDIT_LOG = []
actions = ["task_created", "task_updated", "task_status_changed", "comment_added", "assignee_added", "assignee_removed",
           "tag_added", "due_date_changed", "priority_changed", "attachment_uploaded", "time_logged", "custom_field_updated"]

for _ in range(500):
    task = random.choice(TASKS)
    action = random.choice(actions)
    member = random.choice(MEMBERS)
    AUDIT_LOG.append({
        "log_id": str(uuid.uuid4())[:12],
        "task_id": task["task_id"],
        "action": action,
        "performed_by": member["member_id"],
        "performed_at": fmt_dt(random_date()),
        "old_value": random.choice(statuses) if "status" in action else "",
        "new_value": random.choice(statuses) if "status" in action else "",
        "workspace_id": task["workspace_id"],
    })

# =============================================================================
# WRITE CSVs
# =============================================================================

def write_csv(filename, data, fieldnames=None):
    if not data:
        return
    filepath = os.path.join(OUTPUT_DIR, filename)
    if not fieldnames:
        fieldnames = data[0].keys()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"  Written: {filepath} ({len(data)} rows)")

print("=" * 60)
print("GENERATING CLICKUP DATA (linked to Google Ads)")
print("=" * 60)

write_csv("clickup_workspaces.csv", WORKSPACES)
write_csv("clickup_members.csv", MEMBERS)
write_csv("clickup_spaces.csv", SPACES)
write_csv("clickup_folders.csv", FOLDERS)
write_csv("clickup_lists.csv", LISTS)
write_csv("clickup_custom_fields.csv", CUSTOM_FIELDS)
write_csv("clickup_tags.csv", TAGS)
write_csv("clickup_tasks.csv", TASKS)
write_csv("clickup_task_assignees.csv", TASK_ASSIGNEES)
write_csv("clickup_task_tags.csv", TASK_TAGS)
write_csv("clickup_task_custom_field_values.csv", TASK_CUSTOM_FIELD_VALUES)
write_csv("clickup_task_dependencies.csv", TASK_DEPENDENCIES)
write_csv("clickup_task_comments.csv", TASK_COMMENTS)
write_csv("clickup_task_time_entries.csv", TASK_TIME_ENTRIES)
write_csv("clickup_task_checklists.csv", TASK_CHECKLISTS)
write_csv("clickup_task_attachments.csv", TASK_ATTACHMENTS)
write_csv("clickup_goals.csv", GOALS)
write_csv("clickup_sprints.csv", SPRINTS)
write_csv("clickup_sprint_tasks.csv", SPRINT_TASKS)
write_csv("clickup_automations.csv", AUTOMATIONS)
write_csv("clickup_views.csv", VIEWS)
write_csv("clickup_integrations.csv", INTEGRATIONS)
write_csv("clickup_audit_log.csv", AUDIT_LOG)
write_csv("clickup_gads_campaigns.csv", GADS_CAMPAIGNS)
write_csv("clickup_gads_ad_groups.csv", GADS_AD_GROUPS)

print("\n" + "=" * 60)
print(f"TOTAL CSVs GENERATED: 25 files in '{OUTPUT_DIR}/' directory")
print(f"TOTAL TASKS: {len(TASKS)}")
print(f"TOTAL COMMENTS: {len(TASK_COMMENTS)}")
print(f"TOTAL TIME ENTRIES: {len(TASK_TIME_ENTRIES)}")
print(f"TOTAL AUDIT LOG ENTRIES: {len(AUDIT_LOG)}")
print("=" * 60)
print("\nRelational links:")
print("  - Tasks → Google Ads Campaigns (via google_ads_campaign_id)")
print("  - Tasks → Ad Groups (via custom field values)")
print("  - Folders → Campaigns (folder.campaign_id)")
print("  - Goals → Campaign KPIs (goal.google_ads_campaign_id)")
print("  - Sprints → Tasks (sprint_tasks junction)")
print("  - Integrations include Google Ads, Analytics, Snowflake sync")

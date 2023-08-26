# thelook_ecommerce_user_engagement
User Engagement Data Model in BigQuery SQL

DATA MODEL EXPLANATION

user_id: This is a unique identifier assigned to each user in your database. It's the primary key that can be used to identify a user in the users table.

user_duration: This field represents the duration in seconds between the user's first and last event. It can help measure user engagement over time.

average_events_per_session: This indicates the average number of events per session for a user, calculated by dividing the total number of events by the total number of sessions. It's useful for understanding the user's activity level during each session.

total_sessions: This is the total number of distinct sessions the user has had. A session usually represents a single period of user engagement.

conversion_rate: This field represents the conversion rate of the user, calculated by dividing the total number of purchases by the total number of sessions. It shows the rate at which sessions are leading to a purchase event.

traffic_source: This indicates the source from where the user was directed to the ecommerce platform. It could be from search engines, social media, email marketing, etc.

sessions_per_source: This field represents the count of sessions per traffic source. It helps understand which traffic sources are contributing to more user sessions.

browser: This shows the browser that the user is using to interact with the website.

sessions_per_browser: This represents the count of sessions per browser. It gives an insight into which browsers are most commonly used by users, which can help in tailoring the user experience per browser.


import os
import pandas as pd
import random
import re
from datetime import date, datetime
from collections import OrderedDict
from flask import Flask, render_template, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash

# --- NEW: BigQuery Imports and Configuration ---
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

app = Flask(__name__, template_folder='templates', static_folder='../static')
app.secret_key = 'a_very_secret_and_secure_key_for_dev_v14_final' 

# --- NEW: BigQuery Client Setup ---
# Set your Project and Dataset ID here.
# The application will use the GOOGLE_APPLICATION_CREDENTIALS environment variable for authentication.
PROJECT_ID = "smashers-webapp" 
DATASET_ID = "smashers_data"    
client = bigquery.Client(project=PROJECT_ID)

# Define full table IDs
USERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.users"
PLAYERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.players"
MATCHES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.matches"
ATTENDANCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.attendance"


# --- NEW: BigQuery Helper Functions ---
def read_from_bq(table_id):
    """Reads an entire table from BigQuery and returns it as a Pandas DataFrame."""
    try:
        query = f"SELECT * FROM `{table_id}`"
        # The .to_dataframe() method handles converting BigQuery types to Pandas dtypes
        df = client.query(query).to_dataframe()
        return df
    except NotFound:
        # This case is primarily for the initial setup before tables are created.
        print(f"Warning: Table {table_id} was not found.")
        if 'users' in table_id: return pd.DataFrame(columns=['username', 'password', 'role', 'name', 'age', 'gender', 'status'])
        elif 'players' in table_id: return pd.DataFrame(columns=['username', 'name', 'age', 'gender', 'wins', 'losses'])
        elif 'matches' in table_id: return pd.DataFrame(columns=['male_player1', 'female_player1', 'male_player2', 'female_player2', 'date', 'game_type', 'status', 'winner_team', 'score', 'remark'])
        elif 'attendance' in table_id: return pd.DataFrame(columns=['date', 'present_players'])
        return pd.DataFrame()

def write_to_bq(df, table_id):
    """Writes a Pandas DataFrame to a BigQuery table, overwriting the existing data."""
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrites the table with the new data
    )
    # Ensure integer columns with potential missing values are handled as nullable integers
    for col in ['age', 'wins', 'losses']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col]).astype('Int64')

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
    except Exception as e:
        print(f"An error occurred while writing to BigQuery table {table_id}: {e}")
        # Optionally, re-raise the exception or handle it as needed
        raise

# --- Helper Function (Unchanged) ---
def generate_remark(score):
    if not score or not isinstance(score, str): return ""
    try:
        games = [int(g) for g in re.findall(r'\d+', score)];
        if len(games) < 2 or len(games) % 2 != 0: return ""
        team1_total = sum(games[::2]); team2_total = sum(games[1::2]);
        difference = abs(team1_total - team2_total)
        if difference <= 2: return "Nice Close Game!"
        elif difference <= 5: return "Well Fought Match!"
        else: return "Decisive Victory!"
    except (ValueError, TypeError): return ""

# --- Main, Auth Routes (Now use BigQuery) ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        users_df = read_from_bq(USERS_TABLE)
        username, password, name, age, gender = request.form['username'], request.form['password'], request.form['name'], request.form['age'], request.form['gender']
        if username in users_df['username'].values:
            flash('Username already exists!', 'error'); return redirect(url_for('register'))
        hashed_password = generate_password_hash(password)
        new_user = pd.DataFrame([[username, hashed_password, 'player', name, age, gender, 'pending']], columns=['username', 'password', 'role', 'name', 'age', 'gender', 'status'])
        write_to_bq(pd.concat([users_df, new_user], ignore_index=True), USERS_TABLE)
        flash('Registration successful! Your account is pending admin approval.', 'success'); return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        users_df = read_from_bq(USERS_TABLE)
        username, password = request.form['username'], request.form['password']
        user = users_df[users_df['username'] == username]
        if not user.empty and check_password_hash(user.iloc[0]['password'], password):
            user_data = user.iloc[0]
            if user_data['status'] == 'approved':
                session['username'], session['role'] = username, user_data['role']
                return redirect(url_for('admin_dashboard' if session['role'] == 'admin' else 'dashboard'))
            elif user_data['status'] == 'pending':
                flash('Your account is still pending approval by an admin.', 'error')
            else:
                flash('Your account has not been approved.', 'error')
            return redirect(url_for('login'))
        flash('Invalid username or password.', 'error'); return redirect(url_for('login'))
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear(); flash('You have been logged out.', 'success'); return redirect(url_for('login'))

# --- Player and Public Routes (Now use BigQuery) ---
@app.route('/dashboard')
def dashboard():
    if 'username' not in session or session.get('role') != 'player': return redirect(url_for('login'))
    players_df, matches_df = read_from_bq(PLAYERS_TABLE), read_from_bq(MATCHES_TABLE)
    username = session['username']
    player_data_series = players_df[players_df['username'] == username]
    if player_data_series.empty:
        flash('Player profile not found. Please contact an admin.', 'error')
        return redirect(url_for('login'))
    player_data = player_data_series.iloc[0]
    
    player_matches_df = matches_df[(matches_df['male_player1'] == username) | (matches_df['female_player1'] == username) | (matches_df['male_player2'] == username) | (matches_df['female_player2'] == username)]
    
    player_first_names = {user: str(name).split()[0] for user, name in players_df.set_index('username')['name'].items()}
    processed_matches = []
    for _, match in player_matches_df.iterrows():
        details = match.to_dict()
        p1, p2, p3, p4 = match['male_player1'], match['female_player1'], match['male_player2'], match['female_player2']
        if username in [p1, p2]:
            details['partner_name'] = player_first_names.get(p2 if username == p1 else p1, "")
            details['opponents_names'] = f"{player_first_names.get(p3, p3)} & {player_first_names.get(p4, p4)}"
        else:
            details['partner_name'] = player_first_names.get(p4 if username == p3 else p3, "")
            details['opponents_names'] = f"{player_first_names.get(p1, p1)} & {player_first_names.get(p2, p2)}"
        processed_matches.append(details)
        
    return render_template('dashboard.html', player=player_data.to_dict(), matches=processed_matches)

@app.route('/rankings')
def rankings():
    players_df = read_from_bq(PLAYERS_TABLE)
    if not players_df.empty:
        # Convert wins/losses to numeric, coercing errors to 0
        players_df['wins'] = pd.to_numeric(players_df['wins'], errors='coerce').fillna(0)
        players_df['losses'] = pd.to_numeric(players_df['losses'], errors='coerce').fillna(0)
        players_df['win_loss_ratio'] = players_df['wins'] / (players_df['wins'] + players_df['losses']).replace(0, 1)
        ranked_players = players_df.sort_values(by='win_loss_ratio', ascending=False)
    else: 
        ranked_players = pd.DataFrame()
    return render_template('rankings.html', players=ranked_players.to_dict('records'))

# ... The rest of your routes will follow the same pattern ...
# ... Replace read_csv with read_from_bq and write_csv with write_to_bq ...

# --- Admin Routes (Now use BigQuery) ---
@app.route('/admin')
def admin_dashboard():
    if session.get('role') != 'admin': return redirect(url_for('login'))
    matches_df, players_df = read_from_bq(MATCHES_TABLE), read_from_bq(PLAYERS_TABLE)
    player_first_names = {user: str(name).split()[0] for user, name in players_df.set_index('username')['name'].items()}
    # Add a temporary index for the template to use
    display_matches_df = matches_df[matches_df['status'] != 'completed'].reset_index()
    title = "Manage Upcoming & Ongoing Matches"
    display_matches_list = []
    for _, match in display_matches_df.iterrows():
        match_details = match.to_dict()
        match_details['t1_p1_name'] = player_first_names.get(match['male_player1'], match['male_player1'])
        match_details['t1_p2_name'] = player_first_names.get(match['female_player1'], match['female_player1'])
        match_details['t2_p1_name'] = player_first_names.get(match['male_player2'], match['male_player2'])
        match_details['t2_p2_name'] = player_first_names.get(match['female_player2'], match['female_player2'])
        display_matches_list.append(match_details)
    return render_template('admin_dashboard.html', matches=display_matches_list, title=title)
    
@app.route('/admin/approve_registration/<username>')
def approve_registration(username):
    if session.get('role') != 'admin': return redirect(url_for('login'))
    users_df = read_from_bq(USERS_TABLE)
    players_df = read_from_bq(PLAYERS_TABLE)
    
    user_index = users_df[users_df['username'] == username].index
    if not user_index.empty:
        users_df.loc[user_index, 'status'] = 'approved'
        user_data = users_df.loc[user_index].iloc[0]

        # Create player profile if it doesn't already exist
        if username not in players_df['username'].values:
            new_player = pd.DataFrame([[user_data['username'], user_data['name'], user_data['age'], user_data['gender'], 0, 0]], 
                                      columns=['username', 'name', 'age', 'gender', 'wins', 'losses'])
            players_df = pd.concat([players_df, new_player], ignore_index=True)
            write_to_bq(players_df, PLAYERS_TABLE)

        write_to_bq(users_df, USERS_TABLE)
        flash(f"Registration for {username} approved.", 'success')
    else:
        flash("User not found.", 'error')
    return redirect(url_for('manage_registrations'))

@app.route('/admin/finish_match', methods=['POST'])
def finish_match():
    if session.get('role') != 'admin': return redirect(url_for('login'))
    matches_df, players_df = read_from_bq(MATCHES_TABLE), read_from_bq(PLAYERS_TABLE)
    
    # The 'index' from the form now refers to the DataFrame's index, not a fixed file position
    match_index = int(request.form['match_index'])
    winner_team = request.form['winner_team']
    score = request.form['score']
    
    remark = generate_remark(score)
    
    if match_index < len(matches_df):
        matches_df.loc[match_index, ['status', 'winner_team', 'score', 'remark']] = ['completed', winner_team, score, remark]
        match_info = matches_df.loc[match_index]
        
        winners = ([match_info['male_player1'], match_info['female_player1']], [match_info['male_player2'], match_info['female_player2']]) if winner_team == 'Team 1' else ([match_info['male_player2'], match_info['female_player2']], [match_info['male_player1'], match_info['female_player1']])
        
        # Ensure wins/losses columns are numeric
        players_df['wins'] = pd.to_numeric(players_df['wins'], errors='coerce').fillna(0)
        players_df['losses'] = pd.to_numeric(players_df['losses'], errors='coerce').fillna(0)
        
        players_df.loc[players_df['username'].isin(winners[0]), 'wins'] += 1
        players_df.loc[players_df['username'].isin(winners[1]), 'losses'] += 1
        
        write_to_bq(matches_df, MATCHES_TABLE)
        write_to_bq(players_df, PLAYERS_TABLE)
        flash('Match finished and results recorded.', 'success')
    else:
        flash('Failed to record results. Invalid match index.', 'error')
    return redirect(url_for('admin_dashboard'))
    
# --- The rest of your admin routes need the same read/write conversion ---
# --- (manage_registrations, deny_registration, manage_players, delete_player, etc.) ---
# --- For brevity, only the most complex ones are shown fully converted. ---
# --- All other routes are simple replacements of read_csv/write_csv. ---


# --- Main Execution Block ---
# No longer creates files. Creates the admin user in BigQuery if needed.
if __name__ == '__main__':
    # On first run, check if admin user exists in the BigQuery users table
    users_df = read_from_bq(USERS_TABLE)
    if users_df.empty or 'admin' not in users_df['username'].values:
        print("Admin user not found in BigQuery. Creating one...")
        hashed_password = generate_password_hash('adminpass')
        admin_user = pd.DataFrame([['admin', hashed_password, 'admin', 'Admin', 30, 'N/A', 'approved']], 
                                  columns=['username', 'password', 'role', 'name', 'age', 'gender', 'status'])
        
        # Append admin to existing users (if any) and write back
        updated_users_df = pd.concat([users_df, admin_user], ignore_index=True)
        write_to_bq(updated_users_df, USERS_TABLE)
        print("Admin user created successfully.")

    app.run(debug=True)

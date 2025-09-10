import os
import pandas as pd
import random
import re
from datetime import date, datetime
from collections import OrderedDict
from flask import Flask, render_template, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__, template_folder='templates', static_folder='../static')
app.secret_key = 'a_very_secret_and_secure_key_for_dev_v14_final' # Final Version

# Define paths to data files
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
USERS_FILE = os.path.join(DATA_DIR, 'users.csv')
PLAYERS_FILE = os.path.join(DATA_DIR, 'players.csv')
MATCHES_FILE = os.path.join(DATA_DIR, 'matches.csv')
ATTENDANCE_FILE = os.path.join(DATA_DIR, 'attendance.csv')

# --- Helper Functions ---
def read_csv(file_path):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        if 'users' in file_path: return pd.DataFrame(columns=['username', 'password', 'role'])
        elif 'players' in file_path: return pd.DataFrame(columns=['username', 'name', 'age', 'gender', 'wins', 'losses'])
        elif 'matches' in file_path: return pd.DataFrame(columns=['male_player1', 'female_player1', 'male_player2', 'female_player2', 'date', 'game_type', 'status', 'winner_team', 'score', 'remark'])
        elif 'attendance' in file_path: return pd.DataFrame(columns=['date', 'present_players'])
    return pd.read_csv(file_path)

def write_csv(df, file_path):
    df.to_csv(file_path, index=False)

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

# --- Main, Auth Routes (Unchanged) ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        users_df, players_df = read_csv(USERS_FILE), read_csv(PLAYERS_FILE)
        username, password, name, age, gender = request.form['username'], request.form['password'], request.form['name'], request.form['age'], request.form['gender']
        if username in users_df['username'].values:
            flash('Username already exists!', 'error'); return redirect(url_for('register'))
        hashed_password = generate_password_hash(password)
        new_user = pd.DataFrame([[username, hashed_password, 'player']], columns=['username', 'password', 'role'])
        write_csv(pd.concat([users_df, new_user], ignore_index=True), USERS_FILE)
        new_player = pd.DataFrame([[username, name, age, gender, 0, 0]], columns=['username', 'name', 'age', 'gender', 'wins', 'losses'])
        write_csv(pd.concat([players_df, new_player], ignore_index=True), PLAYERS_FILE)
        flash('Registration successful! Please log in.', 'success'); return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        users_df = read_csv(USERS_FILE)
        username, password = request.form['username'], request.form['password']
        user = users_df[users_df['username'] == username]
        if not user.empty and check_password_hash(user.iloc[0]['password'], password):
            session['username'], session['role'] = username, user.iloc[0]['role']
            return redirect(url_for('admin_dashboard' if session['role'] == 'admin' else 'dashboard'))
        flash('Invalid username or password.', 'error'); return redirect(url_for('login'))
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear(); flash('You have been logged out.', 'success'); return redirect(url_for('login'))

# --- Player and Public Routes ---
@app.route('/dashboard')
def dashboard():
    if 'username' not in session or session.get('role') != 'player': return redirect(url_for('login'))
    players_df, matches_df = read_csv(PLAYERS_FILE), read_csv(MATCHES_FILE)
    username = session['username']
    player_data = players_df[players_df['username'] == username].iloc[0]
    
    player_matches_df = matches_df[(matches_df['male_player1'] == username) | (matches_df['female_player1'] == username) | (matches_df['male_player2'] == username) | (matches_df['female_player2'] == username)]
    
    # --- NEW: Process matches to get first names and simplify template logic ---
    player_first_names = {user: name.split()[0] for user, name in players_df.set_index('username')['name'].items()}
    processed_matches = []
    for _, match in player_matches_df.iterrows():
        details = match.to_dict()
        p1, p2, p3, p4 = match['male_player1'], match['female_player1'], match['male_player2'], match['female_player2']
        if username in [p1, p2]: # Player is in Team 1
            details['partner_name'] = player_first_names.get(p2 if username == p1 else p1, "")
            details['opponents_names'] = f"{player_first_names.get(p3, p3)} & {player_first_names.get(p4, p4)}"
        else: # Player is in Team 2
            details['partner_name'] = player_first_names.get(p4 if username == p3 else p3, "")
            details['opponents_names'] = f"{player_first_names.get(p1, p1)} & {player_first_names.get(p2, p2)}"
        processed_matches.append(details)
        
    return render_template('dashboard.html', player=player_data.to_dict(), matches=processed_matches)

@app.route('/rankings')
def rankings():
    players_df = read_csv(PLAYERS_FILE)
    if not players_df.empty:
        players_df['win_loss_ratio'] = players_df['wins'] / (players_df['wins'] + players_df['losses']).replace(0, 1)
        ranked_players = players_df.sort_values(by='win_loss_ratio', ascending=False)
    else: ranked_players = pd.DataFrame()
    return render_template('rankings.html', players=ranked_players.to_dict('records'))

@app.route('/player/<username>')
def player_profile(username):
    players_df, matches_df = read_csv(PLAYERS_FILE), read_csv(MATCHES_FILE)
    player_data = players_df[players_df['username'] == username]
    if player_data.empty: flash('Player not found.', 'error'); return redirect(url_for('rankings'))
    
    completed_matches_df = matches_df[(matches_df['status'] == 'completed') & ((matches_df['male_player1'] == username) | (matches_df['female_player1'] == username) | (matches_df['male_player2'] == username) | (matches_df['female_player2'] == username))]
    
    # --- NEW: Process matches to get first names and simplify template logic ---
    player_first_names = {user: name.split()[0] for user, name in players_df.set_index('username')['name'].items()}
    processed_matches = []
    for _, match in completed_matches_df.iterrows():
        details = match.to_dict()
        p1, p2, p3, p4 = match['male_player1'], match['female_player1'], match['male_player2'], match['female_player2']
        if username in [p1, p2]: # Player is in Team 1
            details['partner_name'] = player_first_names.get(p2 if username == p1 else p1, "")
            details['opponents_names'] = f"{player_first_names.get(p3, p3)} & {player_first_names.get(p4, p4)}"
        else: # Player is in Team 2
            details['partner_name'] = player_first_names.get(p4 if username == p3 else p3, "")
            details['opponents_names'] = f"{player_first_names.get(p1, p1)} & {player_first_names.get(p2, p2)}"
        processed_matches.append(details)

    return render_template('player_profile.html', player=player_data.iloc[0].to_dict(), matches=processed_matches)

@app.route('/ongoing_matches')
def ongoing_matches():
    matches_df, players_df = read_csv(MATCHES_FILE), read_csv(PLAYERS_FILE)
    player_first_names = {user: name.split()[0] for user, name in players_df.set_index('username')['name'].items()}
    today_str = date.today().strftime('%Y-%m-%d')
    todays_ongoing_df = matches_df[(matches_df['date'] == today_str) & (matches_df['status'] == 'ongoing')]
    todays_ongoing_list = []
    for _, match in todays_ongoing_df.iterrows():
        match_details = match.to_dict()
        match_details['t1_p1_name'] = player_first_names.get(match['male_player1'], match['male_player1'])
        match_details['t1_p2_name'] = player_first_names.get(match['female_player1'], match['female_player1'])
        match_details['t2_p1_name'] = player_first_names.get(match['male_player2'], match['male_player2'])
        match_details['t2_p2_name'] = player_first_names.get(match['female_player2'], match['female_player2'])
        todays_ongoing_list.append(match_details)
    return render_template('ongoing_matches.html', matches=todays_ongoing_list)

@app.route('/history', methods=['GET', 'POST'])
def history():
    matches_df, players_df = read_csv(MATCHES_FILE), read_csv(PLAYERS_FILE)
    player_first_names = {user: name.split()[0] for user, name in players_df.set_index('username')['name'].items()}
    completed_matches = matches_df[matches_df['status'] == 'completed'].copy()
    start_date, end_date = request.form.get('start_date'), request.form.get('end_date')
    if start_date and end_date:
        completed_matches = completed_matches[(completed_matches['date'] >= start_date) & (completed_matches['date'] <= end_date)]
    completed_matches = completed_matches.sort_values(by='date', ascending=False)
    grouped_matches = OrderedDict()
    for _, match in completed_matches.iterrows():
        dt_obj = datetime.strptime(match['date'], '%Y-%m-%d')
        formatted_date = dt_obj.strftime('%B %d, %Y (%A)')
        if formatted_date not in grouped_matches: grouped_matches[formatted_date] = []
        team1_p1 = player_first_names.get(match['male_player1'], match['male_player1'])
        team1_p2 = player_first_names.get(match['female_player1'], match['female_player1'])
        team2_p1 = player_first_names.get(match['male_player2'], match['male_player2'])
        team2_p2 = player_first_names.get(match['female_player2'], match['female_player2'])
        try:
            scores = [int(s) for s in re.findall(r'\d+', str(match['score']))]
            score1, score2 = (scores[0], scores[1]) if len(scores) > 1 else (scores[0], 0)
        except (TypeError, ValueError, IndexError): score1, score2 = 0, 0
        match_details = {'game_type': match['game_type']}
        if match['winner_team'] == 'Team 1':
            match_details.update({'winner_p1_name': team1_p1, 'winner_p2_name': team1_p2, 'winner_score': max(score1, score2), 'loser_p1_name': team2_p1, 'loser_p2_name': team2_p2, 'loser_score': min(score1, score2)})
        else:
            match_details.update({'winner_p1_name': team2_p1, 'winner_p2_name': team2_p2, 'winner_score': max(score1, score2), 'loser_p1_name': team1_p1, 'loser_p2_name': team1_p2, 'loser_score': min(score1, score2)})
        grouped_matches[formatted_date].append(match_details)
    return render_template('history.html', grouped_matches=grouped_matches, start_date=start_date, end_date=end_date)

# --- Admin Routes ---
@app.route('/admin')
def admin_dashboard():
    if session.get('role') != 'admin': return redirect(url_for('login'))
    matches_df, players_df = read_csv(MATCHES_FILE), read_csv(PLAYERS_FILE)
    player_first_names = {user: name.split()[0] for user, name in players_df.set_index('username')['name'].items()}
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

# ... [The rest of the admin routes (attendance, create_match, etc.) are unchanged] ...
@app.route('/admin/attendance', methods=['GET', 'POST'])
def attendance():
    if session.get('role') != 'admin': return redirect(url_for('login'))
    today_str = date.today().strftime('%Y-%m-%d')
    players_df, attendance_df = read_csv(PLAYERS_FILE), read_csv(ATTENDANCE_FILE)
    if request.method == 'POST':
        present_players = request.form.getlist('present_players')
        present_players_str = ','.join(present_players)
        today_attendance = attendance_df[attendance_df['date'] == today_str]
        if not today_attendance.empty:
            attendance_df.loc[today_attendance.index, 'present_players'] = present_players_str
        else:
            new_record = pd.DataFrame([[today_str, present_players_str]], columns=['date', 'present_players'])
            attendance_df = pd.concat([attendance_df, new_record], ignore_index=True)
        write_csv(attendance_df, ATTENDANCE_FILE)
        flash('Attendance for today has been saved!', 'success')
        return redirect(url_for('admin_dashboard'))
    male_players, female_players = players_df[players_df['gender'] == 'Male'].to_dict('records'), players_df[players_df['gender'] == 'Female'].to_dict('records')
    today_record = attendance_df[attendance_df['date'] == today_str]
    present_today = today_record.iloc[0]['present_players'].split(',') if not today_record.empty and pd.notna(today_record.iloc[0]['present_players']) else []
    return render_template('attendance.html', male_players=male_players, female_players=female_players, present_today=present_today)

@app.route('/admin/create_match', methods=['GET', 'POST'])
def create_match():
    if session.get('role') != 'admin': return redirect(url_for('login'))
    today_str, players_df, matches_df, attendance_df = date.today().strftime('%Y-%m-%d'), read_csv(PLAYERS_FILE), read_csv(MATCHES_FILE), read_csv(ATTENDANCE_FILE)
    active_matches_df = matches_df[matches_df['status'].isin(['scheduled', 'ongoing'])]
    unavailable_players = list(set(pd.concat([active_matches_df['male_player1'], active_matches_df['female_player1'], active_matches_df['male_player2'], active_matches_df['female_player2']]).tolist())) if not active_matches_df.empty else []
    today_attendance = attendance_df[attendance_df['date'] == today_str]
    present_players_usernames = today_attendance.iloc[0]['present_players'].split(',') if not today_attendance.empty and pd.notna(today_attendance.iloc[0]['present_players']) else players_df['username'].tolist()
    available_usernames = [p for p in present_players_usernames if p not in unavailable_players]
    available_players_df = players_df[players_df['username'].isin(available_usernames)]
    male_players, female_players = available_players_df[available_players_df['gender'] == 'Male'][['username', 'name']].to_dict('records'), available_players_df[available_players_df['gender'] == 'Female'][['username', 'name']].to_dict('records')
    game_number = len(matches_df[matches_df['date'] == today_str]) + 1
    if request.method == 'POST':
        male_player1, female_player1, male_player2, female_player2, date_val, game_type = (request.form.get(k) for k in ['male_player1', 'female_player1', 'male_player2', 'female_player2', 'date', 'game_type'])
        manually_picked = {p for p in [male_player1, female_player1, male_player2, female_player2] if p}
        females_for_random_pool = [p['username'] for p in female_players if p['username'] not in manually_picked]
        if 'randomize1' in request.form:
            if not females_for_random_pool: flash('Not enough unique female players available for random assignment.', 'error'); return redirect(url_for('create_match'))
            female_player1 = random.choice(females_for_random_pool); females_for_random_pool.remove(female_player1)
        if 'randomize2' in request.form:
            if not females_for_random_pool: flash('Not enough unique female players available for random assignment.', 'error'); return redirect(url_for('create_match'))
            female_player2 = random.choice(females_for_random_pool)
        all_players = [male_player1, female_player1, male_player2, female_player2]
        if None in all_players or "" in all_players: flash('All four player slots must be filled.', 'error'); return redirect(url_for('create_match'))
        if len(set(all_players)) < 4: flash('All four players in a match must be unique. A player may have been auto-selected.', 'error'); return redirect(url_for('create_match'))
        new_match = pd.DataFrame([[male_player1, female_player1, male_player2, female_player2, date_val, game_type, 'scheduled', '', '', '']], columns=['male_player1', 'female_player1', 'male_player2', 'female_player2', 'date', 'game_type', 'status', 'winner_team', 'score', 'remark'])
        write_csv(pd.concat([matches_df, new_match], ignore_index=True), MATCHES_FILE)
        flash('Mixed Doubles Match created successfully!', 'success'); return redirect(url_for('admin_dashboard'))
    return render_template('create_match.html', male_players=male_players, female_players=female_players, game_number=game_number, today_str=today_str)

@app.route('/admin/create_custom_match', methods=['GET', 'POST'])
def create_custom_match():
    if session.get('role') != 'admin': return redirect(url_for('login'))
    players_df, matches_df, attendance_df = read_csv(PLAYERS_FILE), read_csv(MATCHES_FILE), read_csv(ATTENDANCE_FILE)
    today_str = date.today().strftime('%Y-%m-%d')
    active_matches_df = matches_df[matches_df['status'].isin(['scheduled', 'ongoing'])]
    unavailable_players = list(set(pd.concat([active_matches_df['male_player1'], active_matches_df['female_player1'], active_matches_df['male_player2'], active_matches_df['female_player2']]).tolist())) if not active_matches_df.empty else []
    today_attendance = attendance_df[attendance_df['date'] == today_str]
    present_players_usernames = today_attendance.iloc[0]['present_players'].split(',') if not today_attendance.empty and pd.notna(today_attendance.iloc[0]['present_players']) else players_df['username'].tolist()
    available_usernames = [p for p in present_players_usernames if p not in unavailable_players]
    available_players = players_df[players_df['username'].isin(available_usernames)][['username', 'name']].to_dict('records')
    if request.method == 'POST':
        t1_p1, t1_p2, t2_p1, t2_p2, date_val, game_type = (request.form.get(k) for k in ['team1_player1', 'team1_player2', 'team2_player1', 'team2_player2', 'date', 'game_type'])
        all_players = [t1_p1, t1_p2, t2_p1, t2_p2]
        if None in all_players or "" in all_players: flash('All four player slots must be filled.', 'error'); return redirect(url_for('create_custom_match'))
        if len(set(all_players)) < 4: flash('All four players in a match must be unique.', 'error'); return redirect(url_for('create_custom_match'))
        new_match = pd.DataFrame([[t1_p1, t1_p2, t2_p1, t2_p2, date_val, game_type, 'scheduled', '', '', '']], columns=['male_player1', 'female_player1', 'male_player2', 'female_player2', 'date', 'game_type', 'status', 'winner_team', 'score', 'remark'])
        write_csv(pd.concat([matches_df, new_match], ignore_index=True), MATCHES_FILE)
        flash('Custom Match created successfully!', 'success'); return redirect(url_for('admin_dashboard'))
    return render_template('create_custom_match.html', available_players=available_players, today_str=today_str)

@app.route('/admin/start_match/<int:match_index>')
def start_match(match_index):
    if session.get('role') != 'admin': return redirect(url_for('login'))
    matches_df = read_csv(MATCHES_FILE)
    if match_index < len(matches_df):
        matches_df.loc[match_index, 'status'] = 'ongoing'; write_csv(matches_df, MATCHES_FILE); flash('Match started!', 'success')
    else: flash('Invalid match index.', 'error')
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/cancel_match/<int:match_index>')
def cancel_match(match_index):
    if session.get('role') != 'admin': return redirect(url_for('login'))
    matches_df = read_csv(MATCHES_FILE)
    if match_index < len(matches_df) and matches_df.loc[match_index, 'status'] == 'scheduled':
        matches_df = matches_df.drop(matches_df.index[match_index]).reset_index(drop=True)
        write_csv(matches_df, MATCHES_FILE)
        flash('Scheduled match has been successfully canceled.', 'success')
    else:
        flash('Could not cancel match. It might already be ongoing or completed.', 'error')
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/finish_match', methods=['POST'])
def finish_match():
    if session.get('role') != 'admin': return redirect(url_for('login'))
    matches_df, players_df = read_csv(MATCHES_FILE), read_csv(PLAYERS_FILE)
    match_index, winner_team, score = int(request.form['match_index']), request.form['winner_team'], request.form['score']
    remark = generate_remark(score)
    if match_index < len(matches_df):
        matches_df.loc[match_index, ['status', 'winner_team', 'score', 'remark']] = ['completed', winner_team, score, remark]
        match_info = matches_df.loc[match_index]
        winners, losers = ([match_info['male_player1'], match_info['female_player1']], [match_info['male_player2'], match_info['female_player2']]) if winner_team == 'Team 1' else ([match_info['male_player2'], match_info['female_player2']], [match_info['male_player1'], match_info['female_player1']])
        players_df.loc[players_df['username'].isin(winners), 'wins'] += 1
        players_df.loc[players_df['username'].isin(losers), 'losses'] += 1
        write_csv(matches_df, MATCHES_FILE); write_csv(players_df, PLAYERS_FILE)
        flash('Match finished and results recorded.', 'success')
    else: flash('Failed to record results. Invalid match index.', 'error')
    return redirect(url_for('admin_dashboard'))

# --- Main Execution Block ---
if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    for file_path, columns in {
        USERS_FILE: ['username', 'password', 'role'],
        PLAYERS_FILE: ['username', 'name', 'age', 'gender', 'wins', 'losses'],
        MATCHES_FILE: ['male_player1', 'female_player1', 'male_player2', 'female_player2', 'date', 'game_type', 'status', 'winner_team', 'score', 'remark'],
        ATTENDANCE_FILE: ['date', 'present_players']
    }.items():
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            pd.DataFrame(columns=columns).to_csv(file_path, index=False)
    
    users_df = read_csv(USERS_FILE)
    if 'admin' not in users_df['username'].values:
        hashed_password = generate_password_hash('adminpass')
        admin_user = pd.DataFrame([['admin', hashed_password, 'admin']], columns=['username', 'password', 'role'])
        write_csv(pd.concat([users_df, admin_user], ignore_index=True), USERS_FILE)
    app.run(debug=True)

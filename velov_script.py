import csv
import json
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from requests.exceptions import RequestException
from datetime import datetime, timezone, timedelta # Ajout pour la gestion du temps
import subprocess
import os

PROJECT_DIR = "/home/sacha/Documents/Velov" # Répertoire racine de votre projet Git
BIKE_STATES_FILE = os.path.join(PROJECT_DIR, "bike_states.json")
GIT_EXECUTABLE = "/usr/bin/git" # Chemin vers l'exécutable git

now = datetime.now()
date_str = now.strftime("%Y-%m-%d")
folder_path = os.path.join("data", date_str)
os.makedirs(folder_path, exist_ok=True)

BIKE_TRIPS_FILE = os.path.join(folder_path, f"velov_{date_str}.csv")

# --- Fonctions existantes (get_auth_token, fetch_stations, process_station_data) ---
# Elles restent globalement les mêmes, assurez-vous que process_station_data retourne
# bien les données de chaque vélo individuellement, y compris 'stationNumber', 'number', 'status'
# et que vous avez accès au nom de la station pour chaque vélo.

def get_auth_token():
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage") # Trés important pour les environnements contraints
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-setuid-sandbox") # Peut être nécessaire sur certains systémes Linux
    opts.add_argument("--remote-debugging-port=9222") # Peut aider à stabiliser, optionnel
    opts.add_argument("--single-process") # Peut réduire l'utilisation de la mémoire, mais peut aussi rendre moins stable. à tester.
    opts.add_argument("--memory-pressure-off") # Expérimental, pourrait aider
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    
    service = Service("/usr/bin/chromedriver")
    
    driver = webdriver.Chrome(service=service ,options=opts)
    wait = WebDriverWait(driver, 10) 
    token = None
    try:
        driver.get("https://velov.grandlyon.com/fr/mapping")
        wait.until(EC.presence_of_element_located((By.ID, "map")))
        
        marker_wait = WebDriverWait(driver, 10) 
        marker = marker_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div[role='button'][aria-label]")))
        
        driver.execute_script("arguments[0].click();", marker)
        
        token_timeout_end = time.monotonic() + 15
        while time.monotonic() < token_timeout_end:
            logs = driver.get_log("performance")
            for entry in logs:
                try:
                    msg = json.loads(entry["message"])["message"]
                    if msg.get("method") == "Network.requestWillBeSent":
                        hdrs = msg["params"]["request"].get("headers", {})
                        auth_val = hdrs.get("Authorization") or hdrs.get("authorization")
                        if auth_val and auth_val.startswith("Taknv1 "):
                            token = auth_val
                            break
                except json.JSONDecodeError:
                    continue
            if token:
                break
            time.sleep(0.5)

        if not token:
            raise RuntimeError("Token introuvable après attente et clic.")
        return token
    finally:
        driver.quit()

def fetch_stations():
    url = "https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json"
    #print(f"Tentative de récupération des stations depuis : {url}")
    try:
        response = requests.get(url, timeout=20) # Augmentation du timeout
        #print(f"Statut de la réponse : {response.status_code}")
        response.raise_for_status() # Lèvera une exception pour les codes d'erreur HTTP
        
        try:
            data = response.json()
        except json.JSONDecodeError as e_json:
            print(f"Erreur de décodage JSON : {e_json}")
            print(f"Contenu de la réponse (premiers 500 caractères) : {response.text[:500]}")
            return []

        values = data.get("values")
        if values is None:
            print("La clé 'values' est absente de la réponse JSON.")
            print(f"Données reçues : {data}")
            return []
        if not isinstance(values, list):
            print(f"La clé 'values' ne contient pas une liste. Type reçu : {type(values)}")
            print(f"Données reçues : {data}")
            return []
            
        stations_list = [{"id": v['"number"'], "nom": v['"name"']} for v in values if '"number"' in v and '"name"' in v]
        if not stations_list and values: # Si 'values' n'était pas vide mais qu'aucune station n'a été extraite
            print("La liste 'values' a été trouvée mais aucune station valide n'a pu en être extraite (vérifiez les clés 'number' et 'name').")
            print(f"Premiers éléments de 'values' (si existent) : {values[:2]}")
        elif not stations_list:
            print("Aucune station trouvée dans les données (la liste 'values' est peut-être vide).")

        return stations_list

    except requests.Timeout:
        print("Erreur lors de la récupération des stations : Timeout.")
        return []
    except requests.RequestException as e:
        print(f"Erreur lors de la récupération des stations : {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Contenu de la réponse d'erreur (premiers 500 caractères) : {e.response.text[:500]}")
        return []
    except Exception as e_generic:
        print(f"Une erreur inattendue est survenue dans fetch_stations : {e_generic}")
        return []

def process_station_data(station_info, session, simples_keys, battery_keys, rating_keys, max_retries=3, initial_backoff=1):
    sid, sname = station_info["id"], station_info["nom"]
    url = f"https://api.cyclocity.fr/contracts/lyon/bikes?stationNumber={sid}"
    # Retournera une liste de dictionnaires de vélos pour cette station, ou une erreur.
    # Chaque dictionnaire vélo doit inclure 'station_nom_pour_ce_velo': sname
    
    current_retry = 0
    backoff_time = initial_backoff
    
    while current_retry <= max_retries:
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            
            raw_json_data = resp.json()
            bikes_list_raw = []
            if isinstance(raw_json_data, list):
                bikes_list_raw = raw_json_data
            elif isinstance(raw_json_data, dict):
                bikes_list_raw = raw_json_data.get("bikes", [])

            # Enrichir chaque vélo avec le nom de sa station actuelle
            enriched_bikes_list = []
            if bikes_list_raw: # S'il y a des vélos
                for b_data in bikes_list_raw:
                    # Copier pour éviter de modifier l'original si réutilisé
                    bike_copy = b_data.copy()
                    bike_copy['current_station_name_in_scan'] = sname 
                    enriched_bikes_list.append(bike_copy)
            else: # Pas de vélos, mais la station existe
                 # On retourne une liste vide pour indiquer que la station a été traitée sans erreur mais sans vélos
                 pass


            return sid, sname, enriched_bikes_list, None # Succès, retourne la liste des vélos enrichis
            
        except RequestException as e:
            if current_retry < max_retries:
                time.sleep(backoff_time)
                backoff_time *= 2
                current_retry += 1
            else:
                return sid, sname, [], f"Max retries exceeded: {e}"
        except Exception as e_other:
            return sid, sname, [], f"Unexpected error: {e_other}"
    return sid, sname, [], "Retry logic failed unexpectedly"


def load_json_data(filepath, default_data=None):
    """Charge des données JSON depuis un fichier."""
    if default_data is None:
        default_data = {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return default_data
    except json.JSONDecodeError:
        print(f"Avertissement: Fichier JSON malformé ou vide : {filepath}. Utilisation des données par défaut.")
        return default_data


def save_json_data(filepath, data):
    """Sauvegarde des données au format JSON dans un fichier."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def append_trip_to_csv(filepath, trip_data_dict):
    """Ajoute un trajet (dictionnaire) à un fichier CSV."""
    fieldnames = ['bike_number', 'start_station_id', 'start_station_name', 
                  'end_station_id', 'end_station_name', 'start_time', 
                  'end_time', 'duration_minutes']
    try:
        file_exists = False
        with open(filepath, 'r', newline='', encoding='utf-8') as f_check:
            if f_check.read(1): # Vérifie si le fichier n'est pas vide
                file_exists = True
    except FileNotFoundError:
        file_exists = False

    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(trip_data_dict)

def git_commit_and_push():
    """Ajoute les fichiers de données et le log, fait un commit avec un message horodaté et un push."""
    
    # Configuration de l'identité Git (peut être nécessaire pour cron)
    # Utiliser --local pour configurer uniquement pour ce dépôt si vous préférez,
    # mais --global est souvent plus simple si l'utilisateur cron est toujours le même.
    try:
        subprocess.run([GIT_EXECUTABLE, 'config', '--global', 'user.email', 'sachamontel@yahoo.fr'], check=True, cwd=PROJECT_DIR)
        subprocess.run([GIT_EXECUTABLE, 'config', '--global', 'user.name', 'Sacha Montel'], check=True, cwd=PROJECT_DIR)
    except subprocess.CalledProcessError as e_config:
        print(f"Erreur lors de la configuration de Git user/email: {e_config}")
        stderr_output = e_config.stderr.decode('utf-8', errors='replace') if e_config.stderr else 'N/A'
        print(f"Sortie d'erreur Git (config): {stderr_output}")
        # Vous pourriez vouloir arrèter ici si la configuration échoue,
        # car le commit échouera probablement aussi.
        # return

    try:
        current_time_utc = datetime.now(timezone.utc)
        commit_message = f"Mise à jour des données Vélo'V - {current_time_utc.strftime('%Y-%m-%d UTC')}"

        # Etape 1: git add
        #print(f"Tentative d'exécution de 'git add' pour : {BIKE_TRIPS_FILE} dans {PROJECT_DIR}")
        add_process = subprocess.run(
            [GIT_EXECUTABLE, 'add', BIKE_TRIPS_FILE],
            capture_output=True, text=True, cwd=PROJECT_DIR # Spécifier le CWD
        )

        if add_process.returncode != 0:
            print(f"Erreur 'git add' - Code de retour: {add_process.returncode}")
            print(f"Sortie standard 'git add':\n{add_process.stdout}")
            print(f"Sortie d'erreur 'git add':\n{add_process.stderr}")
            # Si 'git add' échoue, il est inutile de continuer.
            return 
        else:
            #print("'git add' exécuté avec succès.")

        # Etape 2: Vérifier les changements stagés
        # `git diff --staged --quiet` renvoie 0 s'il n'y a pas de changements stagés, 1 s'il y en a.
        status_check_process = subprocess.run(
            [GIT_EXECUTABLE, 'diff', '--staged', '--quiet'],
            cwd=PROJECT_DIR # Spécifier le CWD
        )
        
        if status_check_process.returncode == 0:
            print("Aucun changement stagé à commiter pour les fichiers de données et log.")
            return
        else:
            #print("Changements stagés détectés, tentative de commit.")

        # Etape 3: git commit
        # Vérifie si un commit pour aujourd'hui a déjà été fait
        log_output = subprocess.run(["git", "log", "--since=midnight", "--pretty=oneline"], capture_output=True, text=True)
        if not log_output.stdout.strip():
            # Pas encore de commit aujourd'hui ? on commit
            commit_process = subprocess.run(
                [GIT_EXECUTABLE, 'commit', '-m', commit_message],
                capture_output=True, text=True, cwd=PROJECT_DIR # Spécifier le CWD
            )
            if commit_process.returncode != 0:
                print(f"Erreur 'git commit' - Code de retour: {commit_process.returncode}")
                print(f"Sortie standard 'git commit':\n{commit_process.stdout}")
                print(f"Sortie d'erreur 'git commit':\n{commit_process.stderr}")
                return
            else:
                #print(f"Commit effectué avec le message : {commit_message}")
            
            # Etape 4: git push
            push_process = subprocess.run(
                [GIT_EXECUTABLE, 'push'],
                capture_output=True, text=True, cwd=PROJECT_DIR # Spécifier le CWD
            )
            if push_process.returncode != 0:
                print(f"Erreur 'git push' - Code de retour: {push_process.returncode}")
                print(f"Sortie standard 'git push':\n{push_process.stdout}")
                print(f"Sortie d'erreur 'git push':\n{push_process.stderr}")
                return
            else:
                #print("Push effectué avec succés.")
                if push_process.stdout: print(f"Sortie standard 'git push':\n{push_process.stdout}")
                if push_process.stderr: print(f"Sortie d'erreur 'git push' (peut contenir des infos utiles même en cas de succès):\n{push_process.stderr}")

    except FileNotFoundError:
        # Cette exception est levée si GIT_EXECUTABLE n'est pas trouvé.
        print(f"Erreur : La commande '{GIT_EXECUTABLE}' n'a pas été trouvée. Assurez-vous que Git est installé et que le chemin est correct.")
    except Exception as e_generic:
        # Intercepter d'autres exceptions potentielles non gérées par les appels subprocess.
        print(f"Une erreur générique et inattendue est survenue dans git_commit_and_push : {e_generic}")


def main():
    current_scan_time_dt = datetime.now()
    current_scan_time_iso = current_scan_time_dt.isoformat()

    # 1. Charger l'état précédent des vélos
    previous_bike_states = load_json_data(BIKE_STATES_FILE, {})
    
    # 2. Récupérer les données actuelles des vélos
    stations = fetch_stations()
    if not stations:
        print("Aucune station n'a pu être récupérée. Arrêt du script.")
        return
    #print(f"{len(stations)} stations trouvées.")
    
    try:
        token = get_auth_token()
        #print(f"Token obtenu.")
    except RuntimeError as e:
        print(f"Erreur critique lors de l'obtention du token: {e}")
        return

    sess = requests.Session()
    sess.headers.update({
        "Accept": "application/vnd.bikes.v4+json",
        "Authorization": token
    })

    current_bikes_scan_data_list = [] 
    MAX_WORKERS = 10
    processed_count = 0
    error_stations_count = 0

    #print(f"Début du scan des vélos avec {MAX_WORKERS} workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_station = {
            executor.submit(process_station_data, sta, sess, [], [], []): sta
            for sta in stations
        }
        for future in as_completed(future_to_station):
            try:
                _station_id, _station_name, station_bikes_list, error_msg = future.result()
                if error_msg:
                    error_stations_count +=1
                else:
                    current_bikes_scan_data_list.extend(station_bikes_list) 
            except Exception as exc: 
                error_stations_count +=1
            finally:
                processed_count += 1
                sys.stdout.write(f"\rStations scannées : {processed_count}/{len(stations)} (Erreurs stations: {error_stations_count})")
                sys.stdout.flush()
    
    #print(f"\nScan terminé. {len(current_bikes_scan_data_list)} vélos trouvés présents dans les stations.")

    # Créer un dictionnaire des vélos actuellement scannés pour un accès facile par numéro de vélo
    current_bikes_map = {str(b.get('number')): b for b in current_bikes_scan_data_list if b.get('number')}

    # 3. Comparer et détecter les trajets
    new_bike_states = {} 

    # D'abord, traiter les vélos qui sont actuellement visibles (donc AVAILABLE)
    for bike_number_str, current_bike_data in current_bikes_map.items():
        current_station_id = str(current_bike_data.get('stationNumber'))
        current_station_name = current_bike_data.get('current_station_name_in_scan', "N/A")
        
        # Statut implicite : AVAILABLE car le vélo est dans le scan actuel
        current_status_in_logic = "AVAILABLE"

        prev_state = previous_bike_states.get(bike_number_str)

        if prev_state:
            prev_status_in_logic = prev_state.get('status') # Statut logique de notre fichier d'état
            prev_station_id = prev_state.get('last_known_station_id')
            prev_station_name = prev_state.get('last_known_station_name', "N/A")
            prev_timestamp_iso = prev_state.get('timestamp')

            if prev_status_in_logic == "RENTED": # Le vélo était en trajet et est maintenant revenu
                start_time_dt = datetime.fromisoformat(prev_timestamp_iso)
                duration_delta = current_scan_time_dt - start_time_dt
                duration_minutes = round(duration_delta.total_seconds() / 60)

                trip = {
                    'bike_number': bike_number_str,
                    'start_station_id': prev_station_id, # Station d'où il a été marqué RENTED
                    'start_station_name': prev_station_name,
                    'end_station_id': current_station_id,
                    'end_station_name': current_station_name,
                    'start_time': prev_timestamp_iso,
                    'end_time': current_scan_time_iso,
                    'duration_minutes': duration_minutes
                }
                append_trip_to_csv(BIKE_TRIPS_FILE, trip)
                # print(f"Vélo {bike_number_str}: TRAJET TERMINÉ {prev_station_name} -> {current_station_name} en {duration_minutes}min")

        # Mettre à jour/ajouter l'état du vélo comme AVAILABLE à sa station actuelle
        new_bike_states[bike_number_str] = {
            "last_known_station_id": current_station_id,
            "last_known_station_name": current_station_name,
            "status": "AVAILABLE", # Statut logique
            "timestamp": current_scan_time_iso
        }

    # Ensuite, vérifier les vélos de l'état précédent qui ne sont PAS dans le scan actuel
    # Ces vélos sont maintenant considérés comme RENTED
    for bike_number_str, prev_state in previous_bike_states.items():
        if bike_number_str not in current_bikes_map: # Le vélo n'est plus visible
            prev_status_in_logic = prev_state.get('status')
            
            if prev_status_in_logic == "AVAILABLE": # Il était disponible et a disparu -> début de trajet
                # print(f"Vélo {bike_number_str}: DÉBUT TRAJET depuis {prev_state.get('last_known_station_name')}")
                new_bike_states[bike_number_str] = {
                    "last_known_station_id": prev_state.get('last_known_station_id'),
                    "last_known_station_name": prev_state.get('last_known_station_name'),
                    "status": "RENTED", # Statut logique
                    "timestamp": current_scan_time_iso # Heure à laquelle on a constaté sa disparition
                }
            elif prev_status_in_logic == "RENTED":
                # On garde les mêmes infos
                new_bike_states[bike_number_str] = prev_state
    
        # Si le vélo est dans current_bikes_map, son état a déjà été mis à jour dans la boucle précédente.

    # 4. Sauvegarder le nouvel état des vélos
    save_json_data(BIKE_STATES_FILE, new_bike_states)
    #print(f"État des vélos sauvegardé dans {BIKE_STATES_FILE}.")
    #print(f"Trajets enregistrés dans {BIKE_TRIPS_FILE} (si détectés).")
    
    # 5. Faire un commit et un push des modifications
    # Le message est maintenant généré directement dans la fonction git_commit_and_push
    git_commit_and_push()

if __name__ == "__main__":
    main()

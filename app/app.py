import os
import sqlite3
from contextlib import closing
import streamlit as st
import pandas as pd
import re
import unicodedata
from collections import defaultdict
from difflib import SequenceMatcher
from streamlit_extras.stylable_container import stylable_container

# ---------- Config ----------
DB_PATH = os.environ.get("HF_DB_PATH", "hellofresh.sqlite")

def conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def run_query(sql: str, params: dict):
    with closing(conn()) as c:
        cur = c.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

def get_difficulties():
    try:
        rows = run_query("SELECT DISTINCT difficulty FROM recipes WHERE difficulty IS NOT NULL ORDER BY difficulty", {})
        return [r["difficulty"] for r in rows if r["difficulty"]]
    except Exception:
        return []

def get_months_from_db():
    try:
        rows = run_query("SELECT DISTINCT nom FROM saison ORDER BY id", {})
        return [r["nom"] for r in rows if r["nom"]]
    except Exception:
        # fallback au cas où
        return ["janvier","février","mars","avril","mai","juin","juillet","août","septembre","octobre","novembre","décembre"]

def get_all_ingredients():
    try:
        rows = run_query("SELECT DISTINCT nom FROM ingredient ORDER BY nom COLLATE NOCASE;", {})
        return [r["nom"] for r in rows if r["nom"]]
    except Exception:
        return []

def macro_score(r, targets):
    def to_float(x):
        try:
            return float(x) if x is not None else None
        except Exception:
            return None

    def penalty(value, min_t, max_t, weight=0):
        value = to_float(value)
        if value is None:
            return 0.2 * weight
        if min_t <= value <= max_t:
            return 0
        range_size = max(max_t - min_t, 1)
        dist = min_t - value if value < min_t else value - max_t
        norm_dist = dist / range_size
        return norm_dist * weight

    weights = {
        "cal": 0.5,
        "prot": 1.5,
        "carb": 1.0,
        "fat": 1.2,
    }

    penalties = [
        penalty(r.get("cal_per_portion"), *targets["cal"], weights["cal"]),
        penalty(r.get("prot_per_portion"), *targets["prot"], weights["prot"]),
        penalty(r.get("carbs_per_portion"), *targets["carb"], weights["carb"]),
        penalty(r.get("fat_per_portion"), *targets["fat"], weights["fat"]),
    ]

    total_weight = sum(weights.values())
    avg_penalty = sum(penalties) / total_weight
    score = max(0, 100 * (1 - avg_penalty))
    return round(score, 1)
        
import re
import unicodedata

def normalize_token(t: str) -> str:
    if not t:
        return t

    # singulier/pluriel simple
    if len(t) > 4 and t.endswith("s"):
        return t[:-1]

    return t

def base_sort_value(r, sort_by):
    def to_num(x):
        try:
            return float(x) if x is not None else None
        except Exception:
            return None

    if sort_by == "Score macro":
        return to_num(r.get("macro_score")) or 0
    elif sort_by == "Ratio prot/cal":
        return to_num(r.get("prot_cal_index")) or 0
    elif sort_by == "Protéines":
        return to_num(r.get("prot_per_portion")) or 0
    elif sort_by == "Score saison":
        return to_num(r.get("season_score")) or 0
    elif sort_by == "Calories":
        # ici plus petit = mieux, donc on inverse
        v = to_num(r.get("cal_per_portion"))
        return -v if v is not None else float("-inf")
    elif sort_by == "Temps de préparation":
        # idem, plus petit = mieux
        v = to_num(r.get("total_time_min"))
        return -v if v is not None else float("-inf")
    elif sort_by == "Difficulté (facile → difficile)":
        diff_map = {
            "très facile": 4,
            "tres facile": 4,
            "facile": 3,
            "moyenne": 2,
            "intermédiaire": 1,
            "intermediaire": 1,
            "difficile": 0,
            "expert": -1,
        }
        return diff_map.get((r.get("difficulty") or "").strip().lower(), -99)
    else:
        return 0

def normalize_text(s: str) -> str:
    if not s:
        return ""

    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")

    # remplacer ponctuation par espaces
    s = re.sub(r"[^a-z0-9\s]", " ", s)

    # mots à ignorer
    stopwords = {
        "avec", "des", "du", "de", "la", "le", "les", "au", "aux", "et",
        "d", "l", "a", "un", "une", "aux","à",
        "express", "minute", "facon", "façon", "saveurs",
        "complet", "complets", "complete", "completes",
    }

    # ingrédients/formats qu'on peut considérer comme variantes faibles
    weak_tokens = {
        "penne", "fusilli", "spaghetti", "linguine", "gnocchi",
        "riz", "nouilles", "pates", "tortelloni", "conchiglie",
    }

    tokens = [
        normalize_token(t)
        for t in s.split()
        if t not in stopwords and t not in weak_tokens
    ]

    return " ".join(tokens)

def deduplicate_similar_recipes(rows):
    seen = set()
    deduped = []

    for r in rows:
        key = recipe_similarity_key(r)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    return deduped

def deduplicate_exact_title(rows):
    seen = set()
    deduped = []

    for r in rows:
        title = (r.get("title") or "").strip().lower()
        if title in seen:
            continue
        seen.add(title)
        deduped.append(r)

    return deduped

def nutrition_signature(r: dict):
    return (
        r.get("cal_per_portion"),
        r.get("prot_per_portion"),
        r.get("carbs_per_portion"),
        r.get("fat_per_portion"),
    )

def title_similarity(a: str, b: str) -> float:
    a_norm = normalize_text(a or "")
    b_norm = normalize_text(b or "")
    return SequenceMatcher(None, a_norm, b_norm).ratio()

def deduplicate_by_nutrition_and_title(rows, threshold=0.75):
    deduped = []

    for r in rows:
        r_sig = nutrition_signature(r)
        r_title = r.get("title") or ""

        is_duplicate = False

        for kept in deduped:
            kept_sig = nutrition_signature(kept)
            kept_title = kept.get("title") or ""

            # mêmes macros + titres très proches
            if r_sig == kept_sig and title_similarity(r_title, kept_title) >= threshold:
                is_duplicate = True
                break

        if not is_duplicate:
            deduped.append(r)

    return deduped

def recipe_similarity_key(r: dict) -> str:
    title = r.get("title") or ""
    norm = normalize_text(title)

    # on garde les 6 premiers tokens triés pour stabiliser
    tokens = sorted(set(norm.split()))
    return " ".join(tokens[:6])


def get_recipes_by_ids(ids):
    if not ids:
        return []

    placeholders = ",".join("?" for _ in ids)
    sql = f"""
    SELECT
        r.id,
        r.nom AS title,
        ru.url,
        r.img_url,
        r.total_time_min,
        r.cal_per_portion,
        r.prot_per_portion,
        r.carbs_per_portion,
        r.fat_per_portion,
        r.difficulty,
        r.servings
    FROM recipes r
    LEFT JOIN recipe_urls ru ON ru.id = r.url_id
    WHERE r.id IN ({placeholders})
    """
    with closing(conn()) as c:
        cur = c.execute(sql, list(ids))
        return [dict(r) for r in cur.fetchall()]
       
def get_ingredients_for_recipe_ids(recipe_ids):
    if not recipe_ids:
        return {}

    placeholders = ",".join("?" for _ in recipe_ids)
    sql = f"""
    SELECT
        ri.id_recipe,
        i.id AS ingredient_id,
        i.nom AS ingredient_name
    FROM recipes_ingredient ri
    JOIN ingredient i ON i.id = ri.id_ingredient
    WHERE ri.id_recipe IN ({placeholders})
    """

    with closing(conn()) as c:
        cur = c.execute(sql, list(recipe_ids))
        rows = [dict(r) for r in cur.fetchall()]

    result = {}
    for row in rows:
        rid = row["id_recipe"]
        result.setdefault(rid, set()).add(row["ingredient_id"])
    return result

def get_ingredients_for_menu(selected_recipes_dict):
    if not selected_recipes_dict:
        return []

    recipe_ids = list(selected_recipes_dict.keys())
    placeholders = ",".join("?" for _ in recipe_ids)

    sql = f"""
    SELECT
        r.id AS recipe_id,
        r.nom AS recipe_title,
        r.servings,
        ri.id_ingredient,
        i.nom AS ingredient_name,
        ri.quantity_value,
        ri.original_text,
        ri.id_unit,
        u.nom AS unit_name
    FROM recipes r
    JOIN recipes_ingredient ri ON ri.id_recipe = r.id
    JOIN ingredient i ON i.id = ri.id_ingredient
    LEFT JOIN unit u ON u.id = ri.id_unit
    WHERE r.id IN ({placeholders})
    """

    with closing(conn()) as c:
        cur = c.execute(sql, recipe_ids)
        rows = [dict(r) for r in cur.fetchall()]

    # recalcul selon nb parts demandé
    for row in rows:
        recipe_id = row["recipe_id"]
        target_parts = selected_recipes_dict.get(recipe_id, row.get("servings") or 1)
        base_parts = row.get("servings") or 1

        try:
            factor = target_parts / base_parts
        except Exception:
            factor = 1

        if row["quantity_value"] is not None:
            row["needed_quantity"] = row["quantity_value"] * factor
        else:
            row["needed_quantity"] = None

        row["target_parts"] = target_parts

    return rows


def aggregate_shopping_list(ingredient_rows):
    aggregated = {}
    raw_lines = []

    for row in ingredient_rows:
        ing_id = row["id_ingredient"]
        unit_id = row["id_unit"]
        key = (ing_id, unit_id)

        qty = row["needed_quantity"]

        if qty is None:
            raw_lines.append({
                "ingredient": row["ingredient_name"],
                "original_text": row["original_text"],
                "recipe": row["recipe_title"],
                "parts": row["target_parts"],
            })
            continue

        if key not in aggregated:
            aggregated[key] = {
                "ingredient": row["ingredient_name"],
                "quantity": 0,
                "unit": row["unit_name"],
            }

        aggregated[key]["quantity"] += qty

    result = list(aggregated.values())

    for r in result:
        r["quantity"] = round(r["quantity"], 2)

    result.sort(key=lambda x: x["ingredient"].lower())

    return result, raw_lines

def rank_recipes_by_menu_similarity(rows, selected_ids, sort_by, limit_n):
    if not selected_ids:
        return rows

    recipe_ingredients = get_ingredients_for_recipe_ids(
        [r["id"] for r in rows] + list(selected_ids)
    )

    menu_ingredient_ids = set()
    for rid in selected_ids:
        menu_ingredient_ids |= recipe_ingredients.get(rid, set())

    ranked = []

    for r in rows:
        rid = r["id"]

        if rid in selected_ids:
            continue

        ing_ids = recipe_ingredients.get(rid, set())
        common_count = len(ing_ids & menu_ingredient_ids)

        r["menu_similarity_score"] = common_count
        ranked.append(r)

    # 1) sélection des meilleures recettes selon un score combiné
    # ici la similarité est prioritaire, puis le tri de base affine
    ranked.sort(
        key=lambda r: (
            -(r.get("menu_similarity_score") or 0),
            -base_sort_value(r, sort_by),
            hash(r["id"]) % 997
        )
    )

    selected = ranked[:int(limit_n)]

    # 2) affichage final selon le tri utilisateur uniquement
    if sort_by == "Ratio prot/cal":
        selected = sort_ratio_desc(selected)
    else:
        selected.sort(key=lambda r: sort_key_for(r, sort_by))

    return selected

def save_week_menu(menu_name, selected_recipes):
    """
    selected_recipes = dict {id_recipe: nb_portions}
    """
    if not menu_name or not menu_name.strip():
        raise ValueError("Le nom du menu est obligatoire.")

    if not selected_recipes:
        raise ValueError("Aucune recette sélectionnée.")

    with closing(conn()) as c:
        cur = c.cursor()

        cur.execute(
            "INSERT INTO week_menu (name) VALUES (?)",
            (menu_name.strip(),)
        )
        week_menu_id = cur.lastrowid

        rows = [
            (week_menu_id, recipe_id, nb_portions)
            for recipe_id, nb_portions in selected_recipes.items()
        ]

        cur.executemany(
            """
            INSERT INTO recipe_week_menu (id_week_menu, id_recipe, nb_portions)
            VALUES (?, ?, ?)
            """,
            rows
        )
        c.commit()
        return week_menu_id

def list_week_menus():
    with closing(conn()) as c:
        cur = c.execute("""
            SELECT id, name, created_at
            FROM week_menu
            ORDER BY created_at DESC, id DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def load_week_menu(menu_id):
    with closing(conn()) as c:
        cur = c.execute("""
            SELECT id_recipe, nb_portions
            FROM recipe_week_menu
            WHERE id_week_menu = ?
        """, (menu_id,))
        rows = cur.fetchall()

    return {row["id_recipe"]: row["nb_portions"] for row in rows}


def get_week_menu_recipe_details(menu_id):
    with closing(conn()) as c:
        cur = c.execute("""
            SELECT
                r.id,
                r.nom AS title,
                ru.url,
                r.img_url,
                r.total_time_min,
                r.cal_per_portion,
                r.prot_per_portion,
                r.carbs_per_portion,
                r.fat_per_portion,
                r.difficulty,
                r.servings,
                rwm.nb_portions
            FROM recipe_week_menu rwm
            JOIN recipes r ON r.id = rwm.id_recipe
            LEFT JOIN recipe_urls ru ON ru.id = r.url_id
            WHERE rwm.id_week_menu = ?
        """, (menu_id,))
        return [dict(r) for r in cur.fetchall()]


# état global
if "selected_recipes" not in st.session_state:
    st.session_state["selected_recipes"] = {}   # {recipe_id: nb_parts}
    
if "filters_applied" not in st.session_state:
    st.session_state["filters_applied"] = False
    
if "recommend_from_menu" not in st.session_state:
    st.session_state["recommend_from_menu"] = False
    
if "page" not in st.session_state:
    st.session_state["page"] = "browse"
    
if "save_menu_feedback" not in st.session_state:
    st.session_state["save_menu_feedback"] = None
    
if "selected_week_menu_id" not in st.session_state:
    st.session_state["selected_week_menu_id"] = None

@st.dialog("Enregistrer ce menu")
def save_menu_dialog():
    st.write("Quel est le nom de ce menu ?")

    menu_name = st.text_input("Nom du menu", placeholder="Ex : Semaine du 8 avril")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("OK", use_container_width=True):
            try:
                week_menu_id = save_week_menu(
                    menu_name,
                    st.session_state["selected_recipes"]
                )
                st.session_state["save_menu_feedback"] = (
                    "success",
                    f"Menu enregistré avec succès (id={week_menu_id})."
                )
                st.rerun()
            except Exception as e:
                st.session_state["save_menu_feedback"] = (
                    "error",
                    f"Échec de l’enregistrement : {e}"
                )
                st.rerun()

    with col2:
        if st.button("Annuler", use_container_width=True):
            st.rerun()
    
        
# ---------- UI ----------
st.set_page_config(page_title="Planificateur de menus HF", layout="wide")
st.title("Sélection de recettes")

# Ligne d'actions en haut
header_col1, header_col2 = st.columns([1, 6])

main_col, menu_col = st.columns([5, 1], gap="large")

# ---------- Sidebar avec formulaire ----------
with st.sidebar:
    st.header("Filtres")
    with st.form("filters"):
        mois_options = ["(aucun)"] + get_months_from_db()
        mois = st.selectbox("Mois (saisonnalité ingrédient)", mois_options, index=0)
        duree_max = st.number_input("Durée totale max (min)", min_value=10, max_value=240, value=45, step=5)
        kcal_min, kcal_max = st.slider("Calories / portion", min_value=200, max_value=1200, value=(600, 900), step=50)
        prot_min, prot_max = st.slider("Protéines / portion", min_value=0, max_value=80, value=(28, 40), step=1)
        gluc_min, gluc_max = st.slider("Glucides / portion", min_value=0, max_value=150, value=(55, 80), step=1)
        lip_min, lip_max = st.slider("Lipides / portion", min_value=0, max_value=80, value=(16, 31), step=1)

        diffs = ["(toutes)"] + get_difficulties()
        difficulty = st.selectbox("Difficulté", diffs, index=0)

        all_ingredients = get_all_ingredients()

        include_ing = st.multiselect(
            "Doit contenir (ingrédient(s))",
            options=all_ingredients,
            default=[],
            help="Tu peux taper pour chercher un ingrédient et en sélectionner plusieurs."
        )

        exclude_ing = st.multiselect(
            "Exclure (ingrédient(s))",
            options=all_ingredients,
            default=[],
            help="Tu peux taper pour chercher un ingrédient et en sélectionner plusieurs."
        )

        limit = st.number_input("Nombre max de recettes", min_value=5, max_value=200, value=50, step=5)
        sort_by = st.selectbox(
            "Trier par",
            [
                "Score saison",
                "Temps de préparation",
                "Calories",
                "Ratio prot/cal",
                "Score macro",
                "Protéines",
                "Difficulté (facile → difficile)"
            ],
            index=0,
        )

        submitted = st.form_submit_button("Appliquer les filtres")
        if submitted:
            st.session_state["filters_applied"] = True


# ---------- Construction des paramètres ----------
mois_param = None if mois == "(aucun)" else mois
diff_param = None if difficulty == "(toutes)" else difficulty

# Pour simplifier la gestion des multiples ingrédients, on construira
# les conditions directement dans le SQL plus bas.
params = {
    "mois": mois_param,
    "duree_max": int(duree_max),
    "kcal_min": int(kcal_min),
    "kcal_max": int(kcal_max),
    "difficulty": diff_param,
    "limit": int(limit),
}


sql = """
WITH base AS (
  SELECT r.id, r.nom AS title, ru.url, r.img_url, r.total_time_min,
        r.cal_per_portion, r.prot_per_portion, r.carbs_per_portion,
        r.fat_per_portion, r.difficulty, r.servings,
        (1000.0 * r.prot_per_portion) / NULLIF(r.cal_per_portion, 0) AS prot_cal_index

  FROM recipes r
  LEFT JOIN recipe_urls ru ON ru.id = r.url_id
),
ing_counts AS (
  SELECT ri.id_recipe AS rid, COUNT(DISTINCT ri.id_ingredient) AS n_ing
  FROM recipes_ingredient ri
  GROUP BY ri.id_recipe
),
ing_in_season AS (
  SELECT ri.id_recipe AS rid, COUNT(DISTINCT ri.id_ingredient) AS n_season
  FROM recipes_ingredient ri
  JOIN ingredient_saison isz ON isz.id_ingredient = ri.id_ingredient
  JOIN saison s              ON s.id = isz.id_saison
  WHERE (:mois IS NOT NULL) AND s.nom = :mois
  GROUP BY ri.id_recipe
)
SELECT
  b.*,
  COALESCE(
    CASE WHEN :mois IS NULL THEN 0.0
         ELSE CAST(ss.n_season AS REAL) / NULLIF(ic.n_ing, 0)
    END, 0.0
  ) AS season_ratio,
  ROUND(
    100.0 * COALESCE(
      CASE WHEN :mois IS NULL THEN 0.0
           ELSE CAST(ss.n_season AS REAL) / NULLIF(ic.n_ing, 0)
      END, 0.0
    )
  ) AS season_score
FROM base b
LEFT JOIN ing_counts    ic ON ic.rid = b.id
LEFT JOIN ing_in_season ss ON ss.rid = b.id
WHERE 1=1
  AND (:duree_max IS NULL OR b.total_time_min  <= :duree_max)
  AND (:kcal_min  IS NULL OR b.cal_per_portion >= :kcal_min)
  AND (:kcal_max  IS NULL OR b.cal_per_portion <= :kcal_max)
  AND (:difficulty IS NULL OR b.difficulty = :difficulty)
"""

params = {
    "mois": mois_param,
    "duree_max": int(duree_max),
    "kcal_min": int(kcal_min),
    "kcal_max": int(kcal_max),
    "difficulty": diff_param,
    "limit": int(limit),
}


def f_ratio(r):
    try:
        v = r.get("prot_cal_index")
        return float(v) if v is not None else None
    except Exception:
        return None

def sort_ratio_desc(rows):
    with_val  = [r for r in rows if f_ratio(r) is not None]
    without   = [r for r in rows if f_ratio(r) is None]
    with_val.sort(key=lambda r: f_ratio(r), reverse=True)   # décroissant strict
    return with_val + without



# ---------- Exécution conditionnelle ----------
if st.session_state["page"] == "browse":
    # toute ta page actuelle :
    # main_col / menu_col / cartes / menu sticky
    rows = []
    with header_col1:
        if st.button("📚 Menus enregistrés", use_container_width=True):
            st.session_state["page"] = "saved_menus"
            st.rerun()
            
    if st.session_state["filters_applied"]:
        try:
            sql_dynamic = sql

            # inclusions (tous doivent être présents)
            if include_ing:
                inc_clauses = []
                for idx, ing in enumerate(include_ing):
                    key = f"inc_{idx}"
                    params[key] = f"%{ing}%"
                    inc_clauses.append(
                        f"""EXISTS (
                            SELECT 1 FROM recipes_ingredient ri
                            JOIN ingredient i ON i.id = ri.id_ingredient
                            WHERE ri.id_recipe = b.id
                                AND i.nom LIKE :{key} COLLATE NOCASE
                            )"""
                    )
                sql_dynamic += "\nAND " + " AND ".join(inc_clauses)

            # exclusions (aucun ne doit être présent)
            if exclude_ing:
                exc_clauses = []
                for idx, ing in enumerate(exclude_ing):
                    key = f"exc_{idx}"
                    params[key] = f"%{ing}%"
                    exc_clauses.append(
                        f"""NOT EXISTS (
                            SELECT 1 FROM recipes_ingredient ri
                            JOIN ingredient i ON i.id = ri.id_ingredient
                            WHERE ri.id_recipe = b.id
                                AND i.nom LIKE :{key} COLLATE NOCASE
                            )"""
                    )
                sql_dynamic += "\nAND " + " AND ".join(exc_clauses)



            

            def to_num(x):
                try:
                    return float(x)
                except Exception:
                    return None

            def diff_key(x: str):
                if not x:
                    return 99
                m = {
                    "très facile": 0, "tres facile": 0,
                    "facile": 1,
                    "moyenne": 2, "intermédiaire": 2, "intermediaire": 2,
                    "difficile": 3, "expert": 4,
                }
                return m.get(x.strip().lower(), 98)

            def sort_key_for(r, sort_by: str):
                if sort_by == "Ratio prot/cal":
                    v = f_ratio(r)
                    return (v is None, -(v or 0), hash(r["id"]) % 997)
                elif sort_by == "Temps de préparation":
                    return (to_num(r.get("total_time_min")) or 1e9, hash(r["id"]) % 997)
                elif sort_by == "Calories":
                    return (to_num(r.get("cal_per_portion")) or 1e9, hash(r["id"]) % 997)
                elif sort_by == "Protéines":
                    return (-(to_num(r.get("prot_per_portion")) or -1e9), hash(r["id"]) % 997)
                elif sort_by == "Difficulté (facile → difficile)":
                    return (diff_key(r.get("difficulty")), hash(r["id"]) % 997)
                elif sort_by == "Score saison":
                    return (-(r.get("season_score") or 0), hash(r["id"]) % 997)
                elif sort_by == "Score macro":
                    v = r.get("macro_score")
                    return (v is None, -(v or 0), hash(r["id"]) % 997)
                else:
                    return (hash(r["id"]) % 997,)


            from collections import defaultdict

            def pick_best_by_season_then_sort(rows, sort_by: str, limit_n: int):
                buckets = defaultdict(list)
                for r in rows:
                    buckets[r.get("season_score") or 0].append(r)

                season_levels = sorted(buckets.keys(), reverse=True)

                selected = []
                remaining = int(limit_n)

                for s in season_levels:
                    if remaining <= 0:
                        break

                    group = buckets[s]

                    if sort_by == "Ratio prot/cal":
                        group = sort_ratio_desc(group)
                    else:
                        group.sort(key=lambda r: sort_key_for(r, sort_by))

                    take = group[:remaining]
                    selected.extend(take)
                    remaining -= len(take)

                # tri final d'affichage : uniquement par le critère choisi
                if sort_by == "Ratio prot/cal":
                    selected = sort_ratio_desc(selected)
                else:
                    selected.sort(key=lambda r: sort_key_for(r, sort_by))

                return selected


            rows = run_query(sql_dynamic, params)

            # Score macro pour TOUTES les recettes (utile pour le tri et le debug visuel)
            targets = {
                "cal": (kcal_min, kcal_max),
                "prot": (prot_min, prot_max),
                "carb": (gluc_min, gluc_max),
                "fat": (lip_min, lip_max),
            }
            for r in rows:
                r["macro_score"] = macro_score(r, targets)

            if mois_param and rows:
                # on prend plus large avant dédoublonnage pour éviter de tomber sous la limite
                rows = pick_best_by_season_then_sort(rows, sort_by, int(limit) * 3)
            else:
                if sort_by == "Ratio prot/cal":
                    rows = sort_ratio_desc(rows)
                else:
                    rows.sort(key=lambda r: sort_key_for(r, sort_by))

            # dédoublonnage exact puis similaire
            rows = deduplicate_exact_title(rows)
            rows = deduplicate_similar_recipes(rows)
            rows = deduplicate_by_nutrition_and_title(rows, threshold=0.75)
            
            if st.session_state["recommend_from_menu"] and st.session_state["selected_recipes"]:
                rows = rank_recipes_by_menu_similarity(
                    rows,
                    st.session_state["selected_recipes"],
                    sort_by,
                    int(limit)
                )
            else:
                rows = rows[:int(limit)]
            # limite finale
            rows = rows[:int(limit)]


        except Exception as e:
            st.error(f"Erreur SQL : {e}")
            st.stop()
        

    with main_col:
        st.markdown('<div class="main-with-menu">', unsafe_allow_html=True)

        st.subheader(
            f"Résultats ({len(rows)})"
            if st.session_state["filters_applied"]
            else "Applique les filtres pour lancer la recherche"
        )
        title_text = "Suggestions proches de votre menu" if st.session_state["recommend_from_menu"] else f"Résultats ({len(rows)})"
        
        if st.session_state["filters_applied"]:
            if not rows:
                st.info("Aucune recette ne correspond à ces filtres.")
            else:
                # Un peu de style pour les badges
                # ---------- Style des cartes ----------
                st.markdown("""
        <style>
        .grid-cards{
        display:grid;
        grid-template-columns: repeat(5, 1fr); /* desktop */
        gap:18px;
        }

        /* responsive columns */
        @media (max-width: 1400px){ .grid-cards{ grid-template-columns: repeat(4, 1fr);} }
        @media (max-width: 1100px){ .grid-cards{ grid-template-columns: repeat(3, 1fr);} }
        @media (max-width: 800px) { .grid-cards{ grid-template-columns: repeat(2, 1fr);} }
        @media (max-width: 560px) { .grid-cards{ grid-template-columns: 1fr; } }

        .card{
        background:#1f232a; border:1px solid #2a2f3a; border-radius:14px; padding:10px;
        box-shadow:0 6px 20px rgba(0,0,0,.2); transition:transform .08s, box-shadow .2s, border-color .2s;
        display:flex; flex-direction:column;
        }
        .card:hover{ transform:translateY(-2px); box-shadow:0 10px 26px rgba(0,0,0,.3); border-color:#3a4150; }

        /* image: hauteur stable */
        .img-wrap{ width:100%; aspect-ratio:16/9; overflow:hidden; border-radius:10px; }
        .img-wrap img{ width:100%; height:100%; object-fit:cover; display:block; }

        .title{ margin:10px 2px 0 2px; }
        .title a{ color:#cbd5e1; text-decoration:none; font-weight:700; }
        .title a:hover{ text-decoration:underline; }



        /* séparateur */
        .sep-line{ height:1px; background:#2a2f3a; margin:10px 0 6px 0; }

        .badges {
        display: flex;
        flex-wrap: wrap;          /* passe à la ligne si nécessaire */
        gap: 6px;                 /* espacement horizontal et vertical */
        align-items: center;
        margin-top: 8px;
        }

        /* chaque badge reste compact et côte à côte */
        .badge {
        display: inline-flex;      /* force l'alignement horizontal */
        align-items: center;
        justify-content: center;
        flex: 0 0 auto;            /* empêche de s'étirer */
        background: #0f172a;
        color: #e5e7eb;
        border: 1px solid #334155;
        border-radius: 999px;
        padding: 2px 10px;
        font-size: 0.80rem;
        white-space: nowrap;       /* pas de retour à la ligne dans un badge */
        }
        .badge.season-ok    { background:#064e3b; border-color:#065f46; }  /* vert foncé */
        .badge.season-mid   { background:#7c2d12; border-color:#9a3412; }  /* brun/orange */
        .badge.season-low   { background:#111827; border-color:#374151; }  /* gris */
        
        .badge.nutriment-ok    { background:#064e3b; border-color:#065f46; }  /* vert foncé, dans les clous */
        .badge.nutriment-high   { background:#7c2d12; border-color:#9a3412; }  /* brun/orange, trop haut */
        .badge.nutriment-low { background:#6b5a00; border-color:#8b7500; } */
        /* (option) enlever les mini toolbars streamlit */
        div[data-testid="stElementToolbar"]{ display:none !important; }
        </style>
        """, unsafe_allow_html=True)
        




                # Grille responsive simple: 5 colonnes
                # 5 colonnes, alignées en haut
                # ouverture de la grille
                # ===== 5 cartes par ligne, responsive et stable =====
                N_COLS = 5

                def to_int(x):
                    try:
                        return int(x) if x is not None else None
                    except:
                        return None

                def fmt(v, suf=""):
                    return f"{v}{suf}" if v is not None else "—"
                def nutrient_badge(value, min_value, max_value, icon, suffix):
                    cls = "nutriment-ok"

                    if value is None:
                        cls = "nutriment-low"
                    elif value < min_value:
                        cls = "nutriment-low"
                    elif value > max_value:
                        cls = "nutriment-high"

                    return f'<span class="badge {cls}">{icon} {fmt(value, suffix)}</span>'
                def make_card_html(r: dict) -> str:
                    img   = r.get("img_url")
                    title = r.get("title") or "Sans titre"
                    url   = r.get("url")

                    if not img or img.strip() == "":
                        img = "https://cdn-icons-png.flaticon.com/128/2253/2253457.png"

                    time_i = to_int(r.get("total_time_min"))
                    diff   = r.get("difficulty")
                    kcal_i = to_int(r.get("cal_per_portion"))
                    prot_i = to_int(r.get("prot_per_portion"))
                    carb_i = to_int(r.get("carbs_per_portion"))
                    fat_i  = to_int(r.get("fat_per_portion"))
                    ratio  = round(1000*prot_i/kcal_i, 1) if (prot_i is not None and kcal_i and kcal_i>0) else None
                    badges_html = "".join([
                        f'<span class="badge">⏱ {fmt(time_i, " min")}</span>',
                        f'<span class="badge">🎯 {diff if diff else "—"}</span>',
                        nutrient_badge(kcal_i, kcal_min, kcal_max, "🔥", " kcal"),
                        nutrient_badge(prot_i, prot_min, prot_max, "🍗", " g prot"),
                        nutrient_badge(carb_i, gluc_min, gluc_max, "🥖", " g gluc"),
                        nutrient_badge(fat_i, lip_min, lip_max, "🧈", " g lip"),
                        f'<span class="badge">⚖️ {ratio if ratio is not None else "—"}</span>',
                    ])

                    macro = r.get("macro_score")
                    badges_html += f'<span class="badge">📊 {macro if macro is not None else "—"}</span>'

                    season_score = r.get("season_score")
                    cls = "season-low"
                    if season_score is not None:
                        if season_score >= 80:
                            cls = "season-ok"
                        elif season_score >= 40:
                            cls = "season-mid"
                    badges_html += f'<span class="badge {cls}">🍁 {season_score or 0}</span>'

                    return f"""
                    <div class="card">
                        <div class="img-wrap"><img src="{img}" alt="image recette"></div>
                        <div class="title title-2lines">{('<a href="'+url+'" target="_blank">'+title+'</a>') if url else title}</div>
                        <div class="sep-line"></div>
                        <div class="badges">{badges_html}</div>
                    </div>
                    """
            

                # Rendu en lignes de 5 colonnes
                # Rendu en lignes de 5 colonnes
                # 4. AFFICHAGE EN GRILLE + BOUTON
                for i in range(0, len(rows), N_COLS):
                    cols = st.columns(N_COLS, vertical_alignment="top")
                    for j in range(N_COLS):
                        k = i + j
                        if k >= len(rows):
                            continue
                        r = rows[k]
                        card_html = make_card_html(r)
                        recipe_id = r["id"]   # adapte si ce n’est pas "id"

                        with cols[j]:
                            st.markdown(card_html, unsafe_allow_html=True)

                            already = recipe_id in st.session_state["selected_recipes"]
                            
                            if already:
                                st.success("Dans le menu ✅")
                                if st.button("Retirer", key=f"rm_{recipe_id}"):
                                    del st.session_state["selected_recipes"][recipe_id]
                                    st.rerun()
                            else:
                                if st.button("Ajouter au menu", key=f"add_{recipe_id}"):
                                    default_parts = int(r.get("servings") or 2)
                                    default_parts = min(max(default_parts, 1), 10)
                                    st.session_state["selected_recipes"][recipe_id] = default_parts
                                    st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)


    with menu_col:
        menu_box = st.container(key="menu_panel")

        with menu_box:
            st.markdown("### 🍽️ Mon menu")

            selected_ids = list(st.session_state["selected_recipes"].keys())
            
            if not selected_ids:
                st.info("Aucune recette sélectionnée")
            else:
                selected_rows = get_recipes_by_ids(selected_ids)

                selected_rows_by_id = {r["id"]: r for r in selected_rows}
                ordered_selected = [
                    selected_rows_by_id[rid]
                    for rid in selected_ids
                    if rid in selected_rows_by_id
                ]

                for r in ordered_selected:
                    img = r.get("img_url")
                    if not img or img.strip() == "":
                        img = "https://cdn-icons-png.flaticon.com/128/2253/2253457.png"

                    st.image(img, width='stretch')
                    st.markdown(f"**{r.get('title', 'Sans titre')}**")
                    current_parts = st.session_state["selected_recipes"].get(r["id"], int(r.get("servings") or 2))
                    new_parts = st.selectbox(
                        "Parts",
                        options=list(range(1, 11)),
                        index=max(0, min(current_parts, 10) - 1),
                        key=f"parts_{r['id']}"
                    )
                    st.session_state["selected_recipes"][r["id"]] = new_parts

                    meta = []
                    if r.get("total_time_min") is not None:
                        meta.append(f"⏱ {int(r['total_time_min'])} min")
                    if r.get("cal_per_portion") is not None:
                        meta.append(f"🔥 {int(r['cal_per_portion'])} kcal")
                    if r.get("prot_per_portion") is not None:
                        meta.append(f"🍗 {int(r['prot_per_portion'])} g prot")

                    if meta:
                        st.caption(" · ".join(meta))

                    if st.button("Retirer du menu", key=f"side_rm_{r['id']}"):
                        st.session_state["selected_recipes"].pop(r["id"], None)
                        st.rerun()

                    st.divider()

                st.markdown(f"**Total : {len(ordered_selected)} recette(s)**")
                if selected_ids:
                    if st.button("Proposer des recettes similaires", key="recommend_from_menu_btn"):
                        st.session_state["recommend_from_menu"] = True
                        st.rerun()

                if st.session_state["recommend_from_menu"]:
                    if st.button("Revenir aux recettes filtrées", key="back_to_filtered_btn"):
                        st.session_state["recommend_from_menu"] = False
                        st.rerun()
                
                if selected_ids:
                    if st.button("🛒 Voir la liste de courses", type="primary", width='stretch'):
                        st.session_state["page"] = "shopping_list"
                        st.rerun()
    st.markdown("""
    <style>
    .st-key-menu_panel {
        position: fixed;
        top: 80px;
        right: 1rem;
        width: 320px;
        max-height: calc(100vh - 100px);
        overflow-y: auto;
        padding-right: 10px;
        z-index: 999;
        background: #0e1117;
    }

    /* On laisse de la place à droite pour éviter que les cartes passent dessous */
    .main-with-menu {
        margin-right: 340px;
    }
    </style>
    """, unsafe_allow_html=True)
elif st.session_state["page"] == "shopping_list":
    shopping_main_col, shopping_menu_col = st.columns([5, 1], gap="large")

    # 1) D'abord le panneau menu à droite
    with shopping_menu_col:
        menu_box = st.container(key="shopping_menu_panel")

        with menu_box:
            st.markdown("### 🍽️ Mon menu")

            selected_ids = list(st.session_state["selected_recipes"].keys())

            if not selected_ids:
                st.info("Aucune recette sélectionnée")
            else:
                selected_rows = get_recipes_by_ids(selected_ids)
                selected_rows_by_id = {r["id"]: r for r in selected_rows}
                ordered_selected = [
                    selected_rows_by_id[rid]
                    for rid in selected_ids
                    if rid in selected_rows_by_id
                ]

                for r in ordered_selected:
                    img = r.get("img_url")
                    if not img or img.strip() == "":
                        img = "https://cdn-icons-png.flaticon.com/128/2253/2253457.png"

                    st.image(img, width='stretch')
                    st.markdown(f"**{r.get('title', 'Sans titre')}**")

                    current_parts = st.session_state["selected_recipes"].get(
                        r["id"], int(r.get("servings") or 2)
                    )

                    new_parts = st.selectbox(
                        "Parts",
                        options=list(range(1, 11)),
                        index=max(0, min(current_parts, 10) - 1),
                        key=f"shopping_parts_{r['id']}"
                    )

                    # mise à jour immédiate de l'état
                    st.session_state["selected_recipes"][r["id"]] = new_parts

                    meta = []
                    if r.get("total_time_min") is not None:
                        meta.append(f"⏱ {int(r['total_time_min'])} min")
                    if r.get("cal_per_portion") is not None:
                        meta.append(f"🔥 {int(r['cal_per_portion'])} kcal")
                    if r.get("prot_per_portion") is not None:
                        meta.append(f"🍗 {int(r['prot_per_portion'])} g prot")

                    if meta:
                        st.caption(" · ".join(meta))

                    if st.button("Retirer du menu", key=f"shopping_rm_{r['id']}"):
                        st.session_state["selected_recipes"].pop(r["id"], None)
                        st.rerun()

                    st.divider()

                st.markdown(f"**Total : {len(ordered_selected)} recette(s)**")

    # 2) Ensuite seulement on calcule la liste de courses
    with shopping_main_col:
        st.markdown('<div class="shopping-main-with-menu">', unsafe_allow_html=True)
        st.title("🛒 Liste de courses")

        top_btn_col1, top_btn_col2 = st.columns([1, 1])

        with top_btn_col1:
            if st.button("⬅ Retour aux recettes", use_container_width=True):
                st.session_state["page"] = "browse"
                st.rerun()

        with top_btn_col2:
            if st.button("💾 Enregistrer", type="primary", use_container_width=True):
                save_menu_dialog()
        feedback = st.session_state.get("save_menu_feedback")

        if feedback:
            level, message = feedback
            if level == "success":
                st.success(message)
            elif level == "error":
                st.error(message)
                
            st.session_state["save_menu_feedback"] = None                
        ingredient_rows = get_ingredients_for_menu(st.session_state["selected_recipes"])
        shopping_list, raw_lines = aggregate_shopping_list(ingredient_rows)

        st.subheader("Ingrédients agrégés")
        if shopping_list:
            df = pd.DataFrame(shopping_list)
            st.dataframe(df, width='stretch')
        else:
            st.info("Aucun ingrédient agrégé disponible.")

        if raw_lines:
            st.subheader("Lignes à vérifier")
            raw_df = pd.DataFrame(raw_lines)
            st.dataframe(raw_df, width='stretch')
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown("""
<style>
.st-key-shopping_menu_panel {
    position: fixed;
    top: 80px;
    right: 1rem;
    width: 320px;
    max-height: calc(100vh - 100px);
    overflow-y: auto;
    padding-right: 10px;
    z-index: 999;
    background: #0e1117;
}

.shopping-main-with-menu {
    margin-right: 340px;
}
</style>
""", unsafe_allow_html=True)
    
elif st.session_state["page"] == "saved_menus":
    st.title("📚 Menus enregistrés")

    top_col1, top_col2 = st.columns([1, 6])

    with top_col1:
        if st.button("⬅ Retour", use_container_width=True):
            st.session_state["page"] = "browse"
            st.rerun()

    menus = list_week_menus()
    st.markdown("""
<style>
.saved-recipe-card {
    background: #1f232a;
    border: 1px solid #2a2f3a;
    border-radius: 12px;
    padding: 10px;
    margin-bottom: 12px;
}
.saved-recipe-title {
    font-weight: 700;
    margin-top: 8px;
    margin-bottom: 6px;
}
.saved-recipe-meta {
    font-size: 0.9rem;
    color: #cbd5e1;
}
.saved-recipe-img img {
    border-radius: 10px;
}
</style>
""", unsafe_allow_html=True)
    if not menus:
        st.info("Aucun menu enregistré pour le moment.")
    else:
        for menu in menus:
            with st.container():
                st.markdown(f"### {menu['name']}")
                st.caption(f"Créé le : {menu['created_at']}")

                details = get_week_menu_recipe_details(menu["id"])

                if details:
                    detail_cols = st.columns(3, vertical_alignment="top")

                    for idx, r in enumerate(details):
                        with detail_cols[idx % 3]:
                            img = r.get("img_url")
                            if not img or img.strip() == "":
                                img = "https://cdn-icons-png.flaticon.com/128/2253/2253457.png"

                            url = r.get("url")
                            title = r.get("title", "Sans titre")
                            portions = r["nb_portions"]

                            title_html = f'<a href="{url}" target="_blank">{title}</a>' if url else title

                            card_html = (
                                f'<div class="saved-recipe-card">'
                                f'<img src="{img}" style="width:100%; border-radius:10px;" />'
                                f'<div class="saved-recipe-title">{title_html}</div>'
                                f'<div class="saved-recipe-meta">🍽️ {portions} portion(s)</div>'
                                f'</div>'
                            )

                            st.markdown(card_html, unsafe_allow_html=True)
                else:
                    st.write("Aucune recette dans ce menu.")

                btn_col1, btn_col2 = st.columns([1, 1])

                with btn_col1:
                    if st.button("Charger ce menu", key=f"load_menu_{menu['id']}", use_container_width=True):
                        st.session_state["selected_recipes"] = load_week_menu(menu["id"])
                        st.session_state["selected_week_menu_id"] = menu["id"]
                        st.session_state["recommend_from_menu"] = False
                        st.session_state["filters_applied"] = True
                        st.session_state["page"] = "browse"
                        st.rerun()

                with btn_col2:
                    if st.button("Voir la liste de courses", key=f"load_menu_shopping_{menu['id']}", use_container_width=True):
                        st.session_state["selected_recipes"] = load_week_menu(menu["id"])
                        st.session_state["selected_week_menu_id"] = menu["id"]
                        st.session_state["recommend_from_menu"] = False
                        st.session_state["page"] = "shopping_list"
                        st.rerun()

                st.divider()
                
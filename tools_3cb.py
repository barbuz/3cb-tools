import logging
import pandas as pd
from pathlib import Path
logging.basicConfig(level=logging.INFO)

def ingest(xlsx_path):
    logging.info(f"Ingesting {xlsx_path}")
    df = pd.read_excel(xlsx_path)
    # Fix extra spaces in column names
    df.columns = df.columns.str.strip()
    # Keep only relevant columns
    df = df[["Decklist", "Opponent Decklist", "Result"]]
    # Convert results to numbers
    pd.set_option('future.no_silent_downcasting', True)
    df = df.replace({"Result": {"Win": 1, "Tie": 0, "Loss": -1}})
    # Fill in values for second match of each game
    df = df.ffill()
    # Average results of same decks
    df = df.groupby(["Decklist", "Opponent Decklist"]).mean()

    # Process decklists
    for deck in df.index.levels[0]:
        try:
            # Load existing results
            deck_db = pd.read_csv(f"database/{deck}.csv")
        except FileNotFoundError:
            # Create new results
            deck_db = pd.DataFrame(columns=["Opponent Decklist", "Result"])
            # Add deck to decklist keeping it sorted and unique
            with open("database/decks.txt", "r+") as f:
                decks = f.read().splitlines()
                decks.append(deck)
                decks = sorted(set(decks))
                f.seek(0)
                f.write("\n".join(decks))

        deck_db = deck_db.set_index("Opponent Decklist")
        # Update results
        for opponent in df.loc[deck].index:
            new_result = df.loc[deck, opponent]["Result"]
            if opponent in deck_db.index:
                old_result = deck_db.loc[opponent, "Result"]
                if new_result != old_result:
                    raise ValueError(f"Conflicting results for {deck} vs {opponent}")
            else:
                deck_db.loc[opponent, "Result"] = new_result
        # Save results
        deck_db.to_csv(f"database/{deck}.csv")


def remove_banlist(deck_db, banlist):
    for deck in deck_db.index:
        cards = deck.split(' | ')
        if any(card in banlist for card in cards):
            deck_db = deck_db.drop(deck)
    return deck_db

def get_deck_score_vs(deck, banlist, opponents=None):
    deck_db = pd.read_csv(f"database/{deck}.csv", index_col=0)
    deck_db = remove_banlist(deck_db, banlist)
    if opponents is not None:
        deck_db = deck_db.filter(items=opponents, axis=0)
    if deck_db.empty:
        return None
    score = deck_db["Result"].sum()
    return score

def get_score_table(decks, gauntlet):
    """
    Returns a DataFrame with the scores of each deck against each deck in the gauntlet
    """
    scores = dict()
    for deck in decks:
        deck_scores = dict()
        deck_db = pd.read_csv(f"database/{deck}.csv", index_col=0)
        for opponent in gauntlet:
            if opponent in deck_db.index:
                score = deck_db.loc[opponent, "Result"]
                deck_scores[opponent] = score
        scores[deck] = deck_scores
    scores = pd.DataFrame.from_dict(scores, orient="index")
    scores["Total"] = scores.sum(axis=1)
    scores = scores.sort_values(by="Total", ascending=False)
    return scores


def get_suggestions(gauntlet, banlist):
    decks = Path("database/decks.txt").read_text().splitlines()
    scores = dict()
    for deck in decks:
        cards = deck.split(' | ')
        if any(card in banlist for card in cards):
            continue
        score = get_deck_score_vs(deck, banlist, gauntlet)
        if score is not None:
            scores[deck] = score
    scores = pd.Series(scores)
    scores = scores.sort_values(ascending=False)
    return scores

def get_results(deck, gauntlet):
    deck_db = pd.read_csv(f"database/{deck}.csv", index_col=0)
    deck_db = deck_db.filter(items=gauntlet, axis=0)
    deck_db = deck_db.sort_values(by="Result", ascending=False)
    return deck_db

def guess_result(deck, opponent):
    guesses = get_guesses(deck, opponent)+[-guess for guess in get_guesses(opponent, deck)]
    if not guesses:
        return 0
    return sum(guesses) / len(guesses)

def get_guesses(deck, opponent):
    # TODO: Implement a better guess
    deck_db = pd.read_csv(f"database/{deck}.csv", index_col=0)
    opponent_cards = opponent.split(' | ')
    guesses = list()
    for opp in deck_db.index:
        opp_cards = opp.split(' | ')
        similarity = 0
        for card in opponent_cards:
            if card in opp_cards:
                opp_cards.remove(card)
                similarity += 1
        guess = deck_db.loc[opp, "Result"]
        for _ in range(similarity):
            guesses.append(guess)
    return guesses

def fill_guesses(table):
    for deck in table.index:
        for opponent in table.columns:
            if pd.isna(table.loc[deck, opponent]):
                table.loc[deck, opponent] = guess_result(deck, opponent)
    return table

if __name__ == "__main__":
    banlist = Path("banlist.txt").read_text().splitlines()
    gauntlet = Path("gauntlet.txt").read_text().splitlines()
    scores = get_suggestions(gauntlet, banlist)
    scores = scores[scores > 0]
    print(scores)
    table = get_score_table(scores.index, gauntlet)
    print(table)
    table.to_csv("suggestions.csv")
    table = fill_guesses(table)
    table["Total"] = table.sum(axis=1)
    table = table.sort_values(by="Total", ascending=False)
    print(table)
    table.to_csv("suggestions_guesses.csv")

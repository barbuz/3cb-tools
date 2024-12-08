import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO)


class Tools3CB:
    def __init__(self, banlist="banlist.txt", database="database"):
        self.banlist = Path(banlist).read_text().splitlines()
        self.database = database
        self.decklist = Path(f"{self.database}/decks.txt").read_text().splitlines()

    def load_deck(self, deck: str):
        """
        Load the results of a deck from the database
        """
        return pd.read_csv(f"{self.database}/{deck}.csv", index_col=0)

    def save_deck(self, deck):
        """
        Save the results of a deck to the database
        """
        deck.to_csv(f"{self.database}/{deck}.csv")
        # Add deck to decklist keeping it sorted and unique
        self.decklist.append(deck)
        self.decklist = sorted(set(self.decklist))
        Path(f"{self.database}/decks.txt").write_text("\n".join(self.decklist))

    @staticmethod
    def load_gauntlet(self, path="gauntlet.txt"):
        return Path(path).read_text().splitlines()

    def ingest(self, xlsx_path):
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
                deck_db = self.load_deck(deck)
            except FileNotFoundError:
                # Create new results
                deck_db = pd.DataFrame(columns=["Opponent Decklist", "Result"])
                deck_db = deck_db.set_index("Opponent Decklist")

            # Update results
            for opponent in df.loc[deck].index:
                new_result = df.loc[deck, opponent]["Result"]
                if opponent in deck_db.index:
                    old_result = deck_db.loc[opponent, "Result"]
                    if new_result != old_result:
                        raise ValueError(
                            f"Conflicting results for {deck} vs {opponent}"
                        )
                else:
                    deck_db.loc[opponent, "Result"] = new_result
            # Save results
            self.save_deck(deck_db)

    def remove_banlist(self, deck_db):
        for deck in deck_db.index:
            cards = deck.split(' | ')
            if any(card in self.banlist for card in cards):
                deck_db = deck_db.drop(deck)
        return deck_db

    def get_suggestions(self, gauntlet, threshold=None):
        """
        Returns a DataFrame with all decks that have a known score above the threshold
        against the gauntlet
        """
        df = pd.DataFrame()
        for deck in gauntlet:
            if deck not in self.decklist:
                raise ValueError(f"Deck {deck} not found in database")
            # Negate values as we are loading the reverse results
            deck_db = -self.load_deck(deck)
            deck_db = self.remove_banlist(deck_db)
            deck_db = deck_db.rename(columns={"Result": deck})
            df = pd.concat([df, deck_db], axis=1)
        df["Total"] = df.sum(axis=1)
        if threshold is not None:
            df = df[df["Total"] > threshold]
        df = df.sort_values(by="Total", ascending=False)
        df.index.name = "Suggested Deck"
        return df

    def guess_result(self, deck, opponent):
        guesses = self.get_guesses(deck, opponent) + [
            -guess for guess in self.get_guesses(opponent, deck)
        ]
        if not guesses:
            return None
        return sum(guesses) / len(guesses)

    def get_guesses(self, deck, opponent):
        # TODO: Implement a better guess
        deck_db = self.load_deck(deck)
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

    def fill_guesses(self, table):
        for deck in table.index:
            for opponent in table.columns:
                if pd.isna(table.loc[deck, opponent]):
                    table.loc[deck, opponent] = self.guess_result(deck, opponent)
        # Recompute totals
        table = table.drop("Total", axis=1)
        table["Total"] = table.sum(axis=1)
        table = table.sort_values(by="Total", ascending=False)
        return table


if __name__ == "__main__":
    tools = Tools3CB()
    gauntlet = tools.load_gauntlet("gauntlet.txt")
    table = tools.get_suggestions(gauntlet, 0)
    print(table)
    table.to_csv("suggestions.csv")
    table = tools.fill_guesses(table)
    print(table)
    table.to_csv("suggestions_guesses.csv")

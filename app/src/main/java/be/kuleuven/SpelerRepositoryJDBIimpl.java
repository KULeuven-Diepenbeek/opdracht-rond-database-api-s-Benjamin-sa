package be.kuleuven;

import java.util.List;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SpelerRepositoryJDBIimpl implements SpelerRepository {
  private final Jdbi jdbi;

  // RowMapper for Speler
  public static class SpelerMapper implements RowMapper<Speler> {
    @Override
    public Speler map(ResultSet rs, StatementContext ctx) throws SQLException {
      return new Speler(
          rs.getInt("tennisvlaanderenid"),
          rs.getString("naam"),
          rs.getInt("punten"));
    }
  }

  // Constructor
  SpelerRepositoryJDBIimpl(String connectionString, String user, String pwd) {
    this.jdbi = Jdbi.create(connectionString, user, pwd);
    this.jdbi.installPlugin(new SqlObjectPlugin());
    this.jdbi.registerRowMapper(new SpelerMapper());
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    try {
      jdbi.useHandle(handle -> {
        handle.createUpdate("INSERT INTO speler (tennisvlaanderenid, naam, punten) VALUES (:id, :naam, :punten)")
            .bind("id", speler.getTennisvlaanderenId())
            .bind("naam", speler.getNaam())
            .bind("punten", speler.getPunten())
            .execute();
      });
    } catch (Exception e) {
      // Check if the error message contains PRIMARY KEY constraint text
      if (e.getMessage() != null && e.getMessage().contains("UNIQUE constraint failed")) {
        throw new RuntimeException("UNIQUE constraint failed: A PRIMARY KEY constraint failed", e);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    return jdbi.withHandle(handle -> {
      return handle.createQuery("SELECT * FROM speler WHERE tennisvlaanderenid = :id")
          .bind("id", tennisvlaanderenId)
          .mapTo(Speler.class)
          .findOne()
          .orElseThrow(() -> new InvalidSpelerException(String.valueOf(tennisvlaanderenId)));
    });
  }

  @Override
  public List<Speler> getAllSpelers() {
    return jdbi.withHandle(handle -> {
      return handle.createQuery("SELECT * FROM speler")
          .mapTo(Speler.class)
          .list();
    });
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    jdbi.useHandle(handle -> {
      int affectedRows = handle.createUpdate("UPDATE speler SET naam = :naam, punten = :punten WHERE tennisvlaanderenid = :id")
          .bind("naam", speler.getNaam())
          .bind("punten", speler.getPunten())
          .bind("id", speler.getTennisvlaanderenId())
          .execute();
      if (affectedRows == 0) {
          throw new InvalidSpelerException(String.valueOf(speler.getTennisvlaanderenId()));
      }
    });
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
     jdbi.useTransaction(handle -> {
         // First delete relations
         handle.createUpdate("DELETE FROM speler_speelt_tornooi WHERE speler = :id")
               .bind("id", tennisvlaanderenid)
               .execute();

         // Then delete the speler
         int affectedRows = handle.createUpdate("DELETE FROM speler WHERE tennisvlaanderenid = :id")
               .bind("id", tennisvlaanderenid)
               .execute();

         if (affectedRows == 0) {
             throw new InvalidSpelerException(String.valueOf(tennisvlaanderenid));
         }
     });
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    String sql = "SELECT t.clubnaam, w.finale, w.winnaar " +
                 "FROM wedstrijd w " +
                 "JOIN tornooi t ON w.tornooi = t.id " +
                 "WHERE (w.speler1 = :id OR w.speler2 = :id) " +
                 "ORDER BY w.finale ASC " +
                 "LIMIT 1";

    return jdbi.withHandle(handle -> {
      return handle.createQuery(sql)
          .bind("id", tennisvlaanderenid)
          .map((rs, ctx) -> {
            String clubnaam = rs.getString("clubnaam");
            int finaleLevel = rs.getInt("finale");
            int winnerId = rs.getInt("winnaar");
            String finalestring;

            if (finaleLevel == 1 && winnerId == tennisvlaanderenid) {
              finalestring = "winst";
            } else if (finaleLevel == 1) {
              finalestring = "finale";
            } else if (finaleLevel == 2) {
              finalestring = "halve finale";
            } else if (finaleLevel <= 4) {
              finalestring = "kwart finale";
            } else {
              finalestring = "lager dan kwart finale";
            }
            return String.format("Hoogst geplaatst in het tornooi van %s met plaats in de %s", clubnaam, finalestring);
          })
          .findOne()
          .orElseGet(() -> {
              String checkRegistrationSql = "SELECT t.clubnaam FROM speler_speelt_tornooi sst JOIN tornooi t ON sst.tornooi = t.id WHERE sst.speler = :id LIMIT 1";
              return handle.createQuery(checkRegistrationSql)
                           .bind("id", tennisvlaanderenid)
                           .mapTo(String.class)
                           .findOne()
                           .map(club -> "Speler ingeschreven voor tornooi van " + club + " maar geen wedstrijden gespeeld.")
                           .orElse("Speler heeft geen rankings.");
          });
    });
  }

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    jdbi.useHandle(handle -> {
        handle.createUpdate("INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (:spelerId, :tornooiId)")
              .bind("spelerId", tennisvlaanderenId)
              .bind("tornooiId", tornooiId)
              .execute();
    });
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    jdbi.useHandle(handle -> {
        handle.createUpdate("DELETE FROM speler_speelt_tornooi WHERE speler = :spelerId AND tornooi = :tornooiId")
              .bind("spelerId", tennisvlaanderenId)
              .bind("tornooiId", tornooiId)
              .execute();
    });
  }
}

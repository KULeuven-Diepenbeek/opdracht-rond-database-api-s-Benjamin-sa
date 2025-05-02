package be.kuleuven;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SpelerRepositoryJDBCimpl implements SpelerRepository {
  private Connection connection;

  // Constructor
  SpelerRepositoryJDBCimpl(Connection connection) {
    this.connection = connection;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    String sql = "INSERT INTO speler (tennisvlaanderenId, naam, punten) VALUES (?, ?, ?)";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setInt(1, speler.getTennisvlaanderenId());
      ps.setString(2, speler.getNaam());
      ps.setInt(3, speler.getPunten());
      ps.executeUpdate();
    } catch (SQLException e) {
      if (e.getErrorCode() == 19) {
          throw new RuntimeException("Speler with id " + speler.getTennisvlaanderenId() + " already exists.", e);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    String sql = "SELECT * FROM speler WHERE tennisvlaanderenId = ?";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setInt(1, tennisvlaanderenId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new Speler(
              rs.getInt("tennisvlaanderenId"),
              rs.getString("naam"),
              rs.getInt("punten"));
        } else {
          throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId)); // Match test expectation
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Speler> getAllSpelers() {
    List<Speler> spelers = new ArrayList<>();
    String sql = "SELECT * FROM speler";
    try (PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        spelers.add(new Speler(
            rs.getInt("tennisvlaanderenId"),
            rs.getString("naam"),
            rs.getInt("punten")));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return spelers;
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    String sql = "UPDATE speler SET naam = ?, punten = ? WHERE tennisvlaanderenId = ?";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setString(1, speler.getNaam());
      ps.setInt(2, speler.getPunten());
      ps.setInt(3, speler.getTennisvlaanderenId());
      int affectedRows = ps.executeUpdate();
      if (affectedRows == 0) {
          throw new InvalidSpelerException(String.valueOf(speler.getTennisvlaanderenId())); // Match test expectation
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
    String deleteRelationSql = "DELETE FROM speler_speelt_tornooi WHERE speler = ?";
    try (PreparedStatement psRelation = connection.prepareStatement(deleteRelationSql)) {
        psRelation.setInt(1, tennisvlaanderenid);
        psRelation.executeUpdate();
    } catch (SQLException e) {
        System.err.println("Error deleting relations for speler " + tennisvlaanderenid + ": " + e.getMessage());
    }

    String sql = "DELETE FROM speler WHERE tennisvlaanderenId = ?";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setInt(1, tennisvlaanderenid);
      int affectedRows = ps.executeUpdate();
       if (affectedRows == 0) {
           throw new InvalidSpelerException(String.valueOf(tennisvlaanderenid)); // Match test expectation
       }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    String sql = "SELECT t.clubnaam, w.finale, w.winnaar " +
                 "FROM wedstrijd w " +
                 "JOIN tornooi t ON w.tornooi = t.id " +
                 "WHERE (w.speler1 = ? OR w.speler2 = ?) " +
                 "ORDER BY w.finale ASC " +
                 "LIMIT 1";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setInt(1, tennisvlaanderenid);
      ps.setInt(2, tennisvlaanderenid);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
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
        } else {
          String checkRegistrationSql = "SELECT t.clubnaam FROM speler_speelt_tornooi sst JOIN tornooi t ON sst.tornooi = t.id WHERE sst.speler = ? LIMIT 1";
          try (PreparedStatement psCheck = connection.prepareStatement(checkRegistrationSql)) {
              psCheck.setInt(1, tennisvlaanderenid);
              try (ResultSet rsCheck = psCheck.executeQuery()) {
                  if (rsCheck.next()) {
                      return "Speler ingeschreven voor tornooi van " + rsCheck.getString("clubnaam") + " maar geen wedstrijden gespeeld.";
                  } else {
                      return "Speler heeft geen rankings.";
                  }
              }
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
     String sql = "INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (?, ?)";
     try (PreparedStatement ps = connection.prepareStatement(sql)) {
         ps.setInt(1, tennisvlaanderenId);
         ps.setInt(2, tornooiId);
         ps.executeUpdate();
         connection.commit(); // Commit transaction
     } catch (SQLException e) {
         throw new RuntimeException(e);
     }
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    String sql = "DELETE FROM speler_speelt_tornooi WHERE speler = ? AND tornooi = ?";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setInt(1, tennisvlaanderenId);
        ps.setInt(2, tornooiId);
        ps.executeUpdate();
        connection.commit(); // Commit transaction
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
  }
}

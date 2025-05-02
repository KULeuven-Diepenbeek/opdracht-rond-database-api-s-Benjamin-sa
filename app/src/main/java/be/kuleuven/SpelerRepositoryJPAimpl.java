package be.kuleuven;

import java.util.Comparator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.NoResultException;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

public class SpelerRepositoryJPAimpl implements SpelerRepository {
  private final EntityManager em;
  public static final String PERSISTANCE_UNIT_NAME = "be.kuleuven.spelerhibernateTest";

  // Constructor
  SpelerRepositoryJPAimpl(EntityManager entityManager) {
    this.em = entityManager;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    EntityTransaction tx = em.getTransaction();
    try {
      tx.begin();
      em.persist(speler);
      tx.commit();
    } catch (Exception e) {
      if (tx.isActive()) tx.rollback();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler speler = em.find(Speler.class, tennisvlaanderenId);
    if (speler == null) {
      throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId));
    }
    return speler;
  }

  @Override
  public List<Speler> getAllSpelers() {
    return em.createQuery("SELECT s FROM Speler s", Speler.class).getResultList();
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    EntityTransaction tx = em.getTransaction();
    try {
      tx.begin();
      Speler existingSpeler = em.find(Speler.class, speler.getTennisvlaanderenId());
      if (existingSpeler == null) {
        if (tx.isActive()) tx.rollback();
        throw new InvalidSpelerException(String.valueOf(speler.getTennisvlaanderenId()));
      }
      existingSpeler.setNaam(speler.getNaam());
      existingSpeler.setPunten(speler.getPunten());
      tx.commit();
    } catch (InvalidSpelerException e) {
      if (tx.isActive()) tx.rollback();
      throw e;
    } catch (Exception e) {
      if (tx.isActive()) tx.rollback();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenId) {
    EntityTransaction tx = em.getTransaction();
    try {
      tx.begin();
      Speler speler = em.find(Speler.class, tennisvlaanderenId);
      if (speler == null) {
        if (tx.isActive()) tx.rollback();
        throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId));
      }

      // Delete related records in speler_speelt_tornooi first with a native query
      em.createNativeQuery("DELETE FROM speler_speelt_tornooi WHERE speler = :id")
        .setParameter("id", tennisvlaanderenId)
        .executeUpdate();

      em.remove(speler);
      tx.commit();
    } catch (InvalidSpelerException e) {
      if (tx.isActive()) tx.rollback();
      throw e;
    } catch (Exception e) {
      if (tx.isActive()) tx.rollback();
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenId) {
    try {
      String nativeSql = "SELECT t.clubnaam, w.finale, w.winnaar " +
                       "FROM wedstrijd w " +
                       "JOIN tornooi t ON w.tornooi = t.id " +
                       "WHERE (w.speler1 = :id OR w.speler2 = :id) " +
                       "ORDER BY w.finale ASC " +
                       "LIMIT 1";
      
      Object[] result = (Object[]) em.createNativeQuery(nativeSql)
                                  .setParameter("id", tennisvlaanderenId)
                                  .getSingleResult();

      String clubnaam = (String) result[0];
      int finaleLevel = ((Number) result[1]).intValue();
      Integer winnerIdResult = result[2] == null ? null : ((Number) result[2]).intValue();
      int winnerId = (winnerIdResult == null) ? -1 : winnerIdResult;

      String finalestring;
      if (finaleLevel == 1 && winnerId == tennisvlaanderenId) {
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
    } catch (NoResultException e) {
      // Check if player is registered for any tournament
      try {
        String checkRegistrationSql = "SELECT t.clubnaam FROM speler_speelt_tornooi sst JOIN tornooi t ON sst.tornooi = t.id WHERE sst.speler = :id LIMIT 1";
        String clubnaam = (String) em.createNativeQuery(checkRegistrationSql)
                                  .setParameter("id", tennisvlaanderenId)
                                  .getSingleResult();
        return "Speler ingeschreven voor tornooi van " + clubnaam + " maar geen wedstrijden gespeeld.";
      } catch (NoResultException e2) {
        return "Speler heeft geen rankings.";
      }
    } catch (Exception e) {
      throw new RuntimeException("Error fetching highest ranking: " + e.getMessage(), e);
    }
  }

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    EntityTransaction tx = em.getTransaction();
    try {
      tx.begin();
      
      // Use native query for direct DB manipulation, avoids the need for Tornooi entity implementation
      em.createNativeQuery("INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (:spelerId, :tornooiId)")
        .setParameter("spelerId", tennisvlaanderenId)
        .setParameter("tornooiId", tornooiId)
        .executeUpdate();
      
      tx.commit();
    } catch (Exception e) {
      if (tx.isActive()) tx.rollback();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    EntityTransaction tx = em.getTransaction();
    try {
      tx.begin();
      
      // Use native query for direct DB manipulation
      em.createNativeQuery("DELETE FROM speler_speelt_tornooi WHERE speler = :spelerId AND tornooi = :tornooiId")
        .setParameter("spelerId", tennisvlaanderenId)
        .setParameter("tornooiId", tornooiId)
        .executeUpdate();
      
      tx.commit();
    } catch (Exception e) {
      if (tx.isActive()) tx.rollback();
      throw new RuntimeException(e);
    }
  }
}
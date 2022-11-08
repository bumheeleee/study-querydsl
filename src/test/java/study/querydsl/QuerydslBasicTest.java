package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static study.querydsl.entity.QMember.member;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {
    @Autowired
    EntityManager em;
    JPAQueryFactory jpaQueryFactory;

    @BeforeEach
    public void before() {
        jpaQueryFactory = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");

        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL(){
        //member1 찾기
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertEquals(findMember.getUsername(),"member1");
    }

    @Test
    public void startQuerydsl(){
        //QMember m = new QMember("m");
        Member member1 = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertEquals(member1.getUsername(), "member1");
    }

    @Test
    public void search(){
        Member findMember = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10))
                )
                .fetchOne();

        assertEquals(findMember.getUsername(), "member1");
        assertEquals(findMember.getAge(), 10);
    }

    @Test
    public void searchParam(){
        Member findMember = jpaQueryFactory
                .select(member)
                .from(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        // ","를 사용하여 and 연산을 할 수 있다.
        assertEquals(findMember.getUsername(), "member1");
        assertEquals(findMember.getAge(), 10);
    }

    @Test
    public void fetchTypeTest(){
        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .fetch();

        Member fetchOne = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        Member fetchFirst = jpaQueryFactory
                .selectFrom(member)
                .fetchFirst();

        //쿼리가 총 2방 나감 -> 총 개수 세는 쿼리, member 찾는 쿼리
        QueryResults<Member> results = jpaQueryFactory
                .selectFrom(member)
                .fetchResults();

        long total = results.getTotal();
        System.out.println("total = " + total);

        System.out.println("-----------------------");

        List<Member> contents = results.getResults();
        for (Member content : contents) {
            System.out.println("content = " + content);
        }

        long count = jpaQueryFactory
                .selectFrom(member)
                .fetchCount();
        System.out.println("count = " + count);


    }

}

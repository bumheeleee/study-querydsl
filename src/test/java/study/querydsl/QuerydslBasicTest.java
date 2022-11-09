package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

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

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순 (desc)
     * 2. 회원 이름 올림차순 (asc)
     * 단 2에서 회원이름이 없으면(null) 마지막에 출력(nulls last)
     */
    @Test
    public void sortTest(){
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> results = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = results.get(0);
        Member member6 = results.get(1);
        Member memberNull = results.get(2);

        assertEquals(member5.getUsername(), "member5");
        assertEquals(member6.getUsername(), "member6");
        assertEquals(memberNull.getUsername(), null);
    }
    
    @Test
    public void pageTest1(){
        List<Member> results = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.asc())
                .offset(0)      // offset : 시작지점
                .limit(2)       // limit :  페이지 사이즈
                .fetch();

        for (Member result : results) {
            System.out.println("result.getUsername() = " + result.getUsername());
        }
        assertEquals(results.size(), 2);
        assertEquals(results.get(0).getUsername(), "member1");
        assertEquals(results.get(1).getUsername(), "member2");
    }

    @Test
    public void pageTest2(){
        QueryResults<Member> results = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.asc())
                .offset(0)      // offset : 시작지점
                .limit(2)       // limit :  페이지 사이즈
                .fetchResults();

        List<Member> contents = results.getResults();
        long total = results.getTotal();

        assertEquals(contents.size(), 2);
        assertEquals(total,4);
    }

    /**
     * 집합함수의 총 모음
     */
    @Test
    public void aggregationTest(){
        List<Tuple> results = jpaQueryFactory
                .select(member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();

        Tuple result = results.get(0);
        System.out.println("result.get(member.count()) = " + result.get(member.count()));
        System.out.println("result.get(member.age.sum()) = " + result.get(member.age.sum()));
        System.out.println("result.get(member.age.avg()) = " + result.get(member.age.avg()));
        System.out.println("result.get(member.age.max()) = " + result.get(member.age.max()));
        System.out.println("result.get(member.age.min()) = " + result.get(member.age.min()));
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구하기
     */
    @Test
    void group() {
        // given
        List<Tuple> result = jpaQueryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        // when
        Tuple team1 = result.get(0);
        Tuple team2 = result.get(1);

        // then
        System.out.println("team1 = " + team1);
        System.out.println("team2 = " + team2);
    }
}

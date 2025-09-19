package com.telcobright.splitverse.config;

import com.telcobright.util.db.repository.MySqlOptimizedRepositoryFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.Properties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * JPA configuration for Split-Verse with db-util integration.
 * Configures Spring Data JPA to use MySqlOptimizedRepository.
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
    basePackages = "com.telcobright.splitverse.repository.jpa",
    repositoryFactoryBeanClass = MySqlOptimizedRepositoryFactory.class  // Use db-util factory
)
public class JpaConfig {

    /**
     * DataSource for testing - connects to MySQL in LXC container
     */
    @Bean
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/split_verse_test?useSSL=false&serverTimezone=UTC&createDatabaseIfNotExist=true");
        config.setUsername("root");
        config.setPassword("123456");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTestQuery("SELECT 1");
        return new HikariDataSource(config);
    }

    /**
     * EntityManagerFactory configuration
     */
    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan("com.telcobright.splitverse.entity.jpa");

        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setShowSql(false);
        vendorAdapter.setGenerateDdl(false);
        em.setJpaVendorAdapter(vendorAdapter);

        Properties jpaProperties = new Properties();
        jpaProperties.setProperty("hibernate.hbm2ddl.auto", "update");
        jpaProperties.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL8Dialect");
        jpaProperties.setProperty("hibernate.jdbc.batch_size", "1000");
        jpaProperties.setProperty("hibernate.order_inserts", "true");
        jpaProperties.setProperty("hibernate.order_updates", "true");
        jpaProperties.setProperty("hibernate.show_sql", "false");
        jpaProperties.setProperty("hibernate.format_sql", "true");

        em.setJpaProperties(jpaProperties);
        return em;
    }

    /**
     * Transaction manager
     */
    @Bean
    public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(emf);
        return transactionManager;
    }
}
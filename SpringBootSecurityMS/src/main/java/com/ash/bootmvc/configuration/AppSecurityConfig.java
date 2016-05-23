package com.ash.bootmvc.configuration;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

@Configuration
@EnableWebSecurity//to enable Spring Security’s web security support and provide the Spring MVC integration
public class AppSecurityConfig extends WebSecurityConfigurerAdapter {
	/**
	 * The configure(HttpSecurity) method defines which URL paths 
	 * should be secured and which should not. Specifically, the 
	 * "/" and "/home" paths are configured to not
	 *  require any authentication. All other paths must be authenticated.
	 */
	 @Autowired

	 DataSource dataSource;
	@Override
    protected void configure(HttpSecurity http) throws Exception {
        http
            .authorizeRequests()
                .antMatchers("/", "/home").permitAll()
                .anyRequest().authenticated()
                .and()
            .formLogin()
                .loginPage("/login")
                .permitAll()
                .and()
            .logout()
                .permitAll();
    }

	
	 @Autowired
	 UserDetailsService userDetailsService;
	  
	 @Autowired
	 public void configAuthentication(AuthenticationManagerBuilder auth) throws Exception { 
		 
	  auth.userDetailsService(userDetailsService).passwordEncoder(passwordencoder());;
	   
	 } 
	 @Bean(name="passwordEncoder")
	    public PasswordEncoder passwordencoder(){
	     return new BCryptPasswordEncoder();
	    }
	 
	/**
	 * As for the configureGlobal(AuthenticationManagerBuilder) method, 
	 * it sets up an in-memory user store with a single user. 
	 * That user is given 
	 * a username of "user", a password of "password", and a role of "USER".
	 * @param auth
	 * @throws Exception
	 */
/*	 @Autowired
	    public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {
		 
		   auth.jdbcAuthentication().dataSource(dataSource)

		   .usersByUsernameQuery(

		    "select username,password, enabled from users where username=?")

		   .authoritiesByUsernameQuery(

		    "select username, role from user_roles where username=?");
		 
	        auth
	            .inMemoryAuthentication()
	                .withUser("user").password("password").roles("USER");
	    }*/
}

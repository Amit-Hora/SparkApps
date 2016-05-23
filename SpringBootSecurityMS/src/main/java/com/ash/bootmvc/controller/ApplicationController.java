package com.ash.bootmvc.controller;

import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

public class ApplicationController {

	/**
	 * 
	 * As configured, Spring Security provides a filter that 
	 * intercepts that request and authenticates the user.
	 *  If the user fails to authenticate, the page is redirected to 
	 *  "/login?error" and our page displays the appropriate error message.
	 *   Upon successfully signing out, our application is sent to
	 *  "/login?logout" and our page displays the appropriate success message.
	 */
	  /*@RequestMapping(value="/login", method=RequestMethod.POST)
	    public String greeting(@RequestParam(value="username") String name,@RequestParam(value="password") String password, Model model) {
	        model.addAttribute("name", name);
	        return "greeting";
	    }*/

	
}


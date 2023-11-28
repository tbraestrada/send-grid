package co.com.transborder.mstarifas.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import co.com.transborder.commons.dto.RespuestaTarifasPricingVigenteDTO;
import co.com.transborder.commons.dto.entity.CommodityDTO;
import co.com.transborder.commons.dto.entity.ConsultaGestionIntervencionPricingDTO;
import co.com.transborder.commons.dto.entity.ConsultaTarifariosPorProductoDTO;
import co.com.transborder.commons.dto.entity.ConsultaTarifasIntervPricingDTO;
import co.com.transborder.commons.dto.entity.IntervencionPricingDTO;
import co.com.transborder.commons.dto.entity.IntervencionTarifasDTO;
import co.com.transborder.commons.dto.entity.IntervencionTarifasPricingDTO;
import co.com.transborder.commons.dto.tarifas.IntervencionControlVigenciasDTO;
import co.com.transborder.commons.enums.entity.EstadoCommodityEnum;
import co.com.transborder.commons.enums.entity.TipoEmbarqueEnum;
import co.com.transborder.commons.enums.utility.TipoMedidaPesoEnum;
import co.com.transborder.mstarifas.services.CotizacionDesglosadaLclService;
import co.com.transborder.mstarifas.services.IntervencionPricingFclService;
import co.com.transborder.mstarifas.services.IntervencionPricingLclService;
import co.com.transborder.mstarifas.services.IntervencionTierFclService;
import co.com.transborder.mstarifas.services.IntervencionTierLclService;
import io.swagger.v3.oas.annotations.Operation;

@RestController
@RequestMapping("/api/v1/tarifa")
public class IntervencionTarifaController {

	@Autowired
	IntervencionPricingFclService servicePricingFcl;
	
	@Autowired
	IntervencionPricingLclService servicePricingLcl;
	
	@Autowired
	IntervencionTierFclService serviceTierFcl;
	
	@Autowired
	IntervencionTierLclService serviceTierLcl;
	
	@Autowired
	CotizacionDesglosadaLclService serviceDesgLcl;
	
	@Operation(summary = "Consultar tarifas para intervención Pricing")
	@PostMapping
	public ResponseEntity<IntervencionTarifasDTO> consultaTarifasIntervPricing(@RequestBody ConsultaTarifasIntervPricingDTO consultaTarifasIntervPricing) {
		IntervencionTarifasDTO result ;
		if(TipoEmbarqueEnum.FCL.equals(consultaTarifasIntervPricing.getTipoEmbarque())) {			
			result = servicePricingFcl.consultaTarifasIntervPricing(consultaTarifasIntervPricing);
		}else {
			result = servicePricingLcl.consultaTarifasIntervPricing(consultaTarifasIntervPricing);
		}
		
		return new ResponseEntity<>(result, new HttpHeaders(), HttpStatus.OK);
	}
	
	
	@Operation(summary = "Intervenir tarifas FCL Pricing Tier")
	@PostMapping("/intervenirPricingTierFcl")
	public ResponseEntity<String> crearTarifasIntervPricingTierFcl(@RequestBody IntervencionTarifasPricingDTO tarifasIntervPricingTierFcl) {
		
		if(tarifasIntervPricingTierFcl.getConsultaTarifasIntervPricingDTO().getTier() != null ) {
			// TIER			
			serviceTierFcl.crearTierFcl(tarifasIntervPricingTierFcl);
		}else {		
			// PRICING
			servicePricingFcl.crearPricingFcl(tarifasIntervPricingTierFcl);
		}
		
		return new ResponseEntity<>("OK", new HttpHeaders(), HttpStatus.OK);	
	}
	
	
	@Operation(summary = "Consultar parametrizacion tarifas FCL y LCL Tier de cliente")
	@PostMapping("/consultaTier")
	public ResponseEntity<IntervencionTarifasDTO> consultaTarifasIntervTier(@RequestBody ConsultaTarifasIntervPricingDTO consultaTarifasIntervTier) {
		IntervencionTarifasDTO result;
		if(TipoEmbarqueEnum.FCL.equals(consultaTarifasIntervTier.getTipoEmbarque())) {
			result = serviceTierFcl.consultaTarifasIntervTier(consultaTarifasIntervTier);
		}else {			
			result = serviceTierLcl.consultaTarifasIntervTier(consultaTarifasIntervTier);
		}
		
		return new ResponseEntity<>(result, new HttpHeaders(), HttpStatus.OK);
	}
	

	@Operation(summary = "Intervenir tarifas LCL Pricing Tier")
	@PostMapping("/intervenirPricingTierLcl")
	public ResponseEntity<String> crearTarifasIntervPricingTierLcl(@RequestBody IntervencionTarifasPricingDTO tarifasIntervPricingTierLcl) {
		if(tarifasIntervPricingTierLcl.getConsultaTarifasIntervPricingDTO().getTier() != null ) {
			serviceTierLcl.crearTierLcl(tarifasIntervPricingTierLcl);
		}else{
			servicePricingLcl.crearPricingLcl(tarifasIntervPricingTierLcl);
		}
		return new ResponseEntity<>("OK", new HttpHeaders(), HttpStatus.OK);
	}
	 
	
	@Operation(summary = "Consultar intervenciones para gestionar por control de vigencia Pricing")
	@PostMapping("/consultaGestionVigenciaPricing")
	public ResponseEntity<List<IntervencionPricingDTO>> consultaGestionVigenciaPricing(@RequestBody ConsultaGestionIntervencionPricingDTO consultaGestionVigenciaPricingDto) {
		return new ResponseEntity<>(servicePricingFcl.consultaGestionVigenciaPricing(consultaGestionVigenciaPricingDto), new HttpHeaders(), HttpStatus.OK);
	}
	
	@Operation(summary = "Actualiza la intervencion de Pricing")
	@PostMapping("/actualizarIntervencionPricing")
	public ResponseEntity<IntervencionPricingDTO> actualizarIntervencionPricing(@RequestBody IntervencionPricingDTO intervencionPricing) {		
		return new ResponseEntity<>(servicePricingFcl.actualizarIntervencionPricing(intervencionPricing), new HttpHeaders(), HttpStatus.OK);
	}
	
	@Operation(summary = "Reporte Intevenciones afectadas pricing")
	@GetMapping("/reporteIntervencionPricingAfectadas")
	public ResponseEntity<List<IntervencionControlVigenciasDTO>> reporteIntervencionPricingAfectadas(IntervencionPricingDTO intervencionPricing) {		
		return new ResponseEntity<>(servicePricingFcl.consultaControlVigenciasIntervencion(), new HttpHeaders(), HttpStatus.OK);
	}
		
	@Operation(summary = "Consultar tarifarios cargados por Producto")
	@PostMapping("/consultarTarifariosPorProducto")
	public ResponseEntity<List<ConsultaTarifariosPorProductoDTO>> consultarTarifariosPorProducto(@RequestBody ConsultaTarifariosPorProductoDTO consultaTarifariosPorProductoDTO) {		
		return new ResponseEntity<>(servicePricingFcl.consultarTarifariosPorProducto(consultaTarifariosPorProductoDTO), new HttpHeaders(), HttpStatus.OK);
	}
	
	@Operation(summary = "Elimina la Intervención Comercial FCL/LCL")
	@PostMapping("/deleteIntervencionComercial/{idCotizacion}")
	public HttpStatus deleteIntervencionComercial(@PathVariable("idCotizacion") Long idCotizacion) {
		servicePricingFcl.deleteIntervencionComercial(idCotizacion);
		return HttpStatus.OK;
	}
	
	@Operation(summary = "Crear, Actualizar Tarifa Específica")
	@PostMapping("/actualizarTarifaEspecifica")
	public HttpStatus actualizaTarifaEspecifica(@RequestBody CommodityDTO commodity) {
		servicePricingFcl.createTarifaEspecifica(commodity);
		return HttpStatus.OK;
	}
	
	@Operation(summary = "Crear, Actualizar Tarifa Específica")
	@GetMapping("/getTarifaEspecificaEstado")
	public ResponseEntity<List<CommodityDTO>> getTarifaEspecificaEstado(@RequestParam("estado") EstadoCommodityEnum estado) {
		return new ResponseEntity<>(servicePricingFcl.getTarifasEspecificasEstado(estado), new HttpHeaders(), HttpStatus.OK);
	}
	
	
	@Operation(summary = "Consulta todas las tarifas Específicas")
		@GetMapping("/getAllTarifasEspecificas")
		public ResponseEntity<List<CommodityDTO>> getAllTarifasEspecificas() {
			return new ResponseEntity<>(servicePricingFcl.getAllTarifasEspecificas(), new HttpHeaders(), HttpStatus.OK);
	}
			
			
	@Operation(summary = "calcula la mínima concepto de tarifa")
	@GetMapping("/getMinima")
	public ResponseEntity<Double> gatMinima(@RequestParam("minima")Double minima, @RequestParam("concepto")double concepto, 
			@RequestParam("tipoMedida")TipoMedidaPesoEnum tipoMedida, @RequestParam("peso")Double peso, @RequestParam("metrosCubicos")Double metrosCubicos) {
		
		return new ResponseEntity<>(serviceDesgLcl.getMinima(minima, concepto, tipoMedida, peso, metrosCubicos), new HttpHeaders(), HttpStatus.OK);
	}
	
	@Operation(summary = "Consultar intervenciones para gestionar por control de vigencia Pricing")
	@GetMapping("/consultaGestionGerentePricing")
	public ResponseEntity<List<RespuestaTarifasPricingVigenteDTO>> consultaGestionGerentePricing(@RequestParam("usuario") String user) {
		return new ResponseEntity<>(servicePricingFcl.consultaPricingGerente(user), new HttpHeaders(), HttpStatus.OK);
	}
	
	@Operation(summary = "Confirma intervenciones para gestionar por control de vigencia Pricing")
	@PostMapping("/confirmarIntervencionPricing")
	public HttpStatus confirmarIntervencionPricing(@RequestBody List<RespuestaTarifasPricingVigenteDTO> lista) {
		servicePricingFcl.confirmarTarifaPrincing(lista);
		return HttpStatus.OK;
	}
}

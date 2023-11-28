package co.com.transborder.mstarifas.services.impl;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.stereotype.Service;
import co.com.transborder.commons.configurations.IAuthenticationFacade;
import co.com.transborder.commons.dto.UserDTO;
import co.com.transborder.commons.dto.cotizaciones.CiudadDTO;
import co.com.transborder.commons.dto.entity.CommodityDTO;
import co.com.transborder.commons.dto.entity.ConsultaGestionIntervencionPricingDTO;
import co.com.transborder.commons.dto.entity.ConsultaTarifariosPorProductoDTO;
import co.com.transborder.commons.dto.entity.ConsultaTarifasIntervPricingDTO;
import co.com.transborder.commons.dto.entity.IntervencionFleteInternacionalFclDTO;
import co.com.transborder.commons.dto.entity.IntervencionTarifaFletesInternacionalLclDTO;

import co.com.transborder.commons.dto.entity.IntervencionGastosDestinoLclDTO;
import co.com.transborder.commons.dto.entity.IntervencionGastosOrigenLclDTO;
import co.com.transborder.commons.dto.entity.IntervencionPricingDTO;
import co.com.transborder.commons.dto.entity.IntervencionRecargosCarrierDestinoFclDTO;
import co.com.transborder.commons.dto.entity.IntervencionRecargosCarrierDestinoLclDTO;
import co.com.transborder.commons.dto.entity.IntervencionRecargosCarrierOrigenFclDTO;
import co.com.transborder.commons.dto.entity.IntervencionRecargosCarrierOrigenLclDTO;
import co.com.transborder.commons.dto.entity.IntervencionTarifaFletesInternacionalLclDTO;
import co.com.transborder.commons.dto.entity.IntervencionTarifasDTO;
import co.com.transborder.commons.dto.entity.IntervencionTarifasPricingDTO;
import co.com.transborder.commons.dto.entity.IntervencionTerrestreDestinoLclDTO;
import co.com.transborder.commons.dto.entity.IntervencionTerrestreOrigenLclDTO;
import co.com.transborder.commons.dto.entity.MonedaDTO;
import co.com.transborder.commons.dto.entity.PuertoDTO;
import co.com.transborder.commons.dto.tarifas.IntervencionControlVigenciasDTO;
import co.com.transborder.commons.dto.tarifas.NavieraDTO;
import co.com.transborder.commons.entities.CommodityEntity;
import co.com.transborder.commons.entities.IntervencionDevolucionContenedorDestinoEntity;
import co.com.transborder.commons.entities.IntervencionGastosDestinoLCLEntity;
import co.com.transborder.commons.entities.IntervencionGastosOrigenLCLEntity;
import co.com.transborder.commons.entities.IntervencionPricingEntity;
import co.com.transborder.commons.entities.IntervencionRecargosCarrierDestinoLclEntity;
import co.com.transborder.commons.entities.IntervencionRecargosCarrierOrigenLclEntity;
import co.com.transborder.commons.entities.IntervencionTarifaFletesInternacionalLclEntity;
import co.com.transborder.commons.entities.IntervencionTarifaFletesInternacionalLclEntity;
import co.com.transborder.commons.entities.IntervencionTerrestreDestinoLclEntity;
import co.com.transborder.commons.entities.IntervencionTerrestreOrigenLclEntity;
import co.com.transborder.commons.enums.entity.CategoriaCargueArchivoEnum;
import co.com.transborder.commons.enums.entity.EstadoCommodityEnum;
import co.com.transborder.commons.enums.entity.FormaIntervencionEnum;
import co.com.transborder.commons.enums.entity.TipoContenedorEnum;
import co.com.transborder.commons.enums.entity.TipoTarifaEnum;
import co.com.transborder.commons.enums.entity.TipoTarifarioEnum;
import co.com.transborder.commons.enums.entity.TipoTransporteEnum;
import co.com.transborder.mstarifas.constants.SqlQueriesConstants;
import co.com.transborder.mstarifas.mappers.IntervencionContenedorFleteInternacionalFclMapper;
import co.com.transborder.mstarifas.mappers.IntervencionDevolucionContenedorDestinoMapper;
import co.com.transborder.mstarifas.mappers.IntervencionGastosDestinoLclMapper;
import co.com.transborder.mstarifas.mappers.IntervencionGastosOrigenLclMapper;
import co.com.transborder.mstarifas.mappers.IntervencionPricingMapper;
import co.com.transborder.mstarifas.mappers.IntervencionRecargosCarrierDestinoLclMapper;
import co.com.transborder.mstarifas.mappers.IntervencionRecargosCarrierOrigenLclMapper;
import co.com.transborder.mstarifas.mappers.IntervencionTarifaFleteInternacionalLclMapper;
import co.com.transborder.mstarifas.mappers.IntervencionTerrestreDestinoLclMapper;
import co.com.transborder.mstarifas.mappers.IntervencionTerrestreOrigenLclMapper;
import co.com.transborder.mstarifas.mappers.TarifaEspecificaMapper;
import co.com.transborder.mstarifas.repositories.ConsultaTarifariosDynamicRepository;
import co.com.transborder.mstarifas.repositories.CotizacionSpRepository;
import co.com.transborder.mstarifas.repositories.EjecucionDiariaTarifasVigentesRepository;
import co.com.transborder.mstarifas.repositories.IntervencionDevolucionContenedorDestinoRepository;
import co.com.transborder.mstarifas.repositories.IntervencionGastosDestinoLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionGastosOrigenLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionPricingDynamicRepository;
import co.com.transborder.mstarifas.repositories.IntervencionPricingFclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionPricingLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionRecargosCarrierDestinoLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionRecargosCarrierOrigenLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionTarifaFleteInternacionalLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionTarifaFleteInternacionalLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionTerrestreDestinoLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionTerrestreDestinoLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionTerrestreOrigenLclRepository;
import co.com.transborder.mstarifas.repositories.IntervencionTerrestreOrigenLclRepository;
import co.com.transborder.mstarifas.repositories.TarifaEspecificaRepository;
import co.com.transborder.mstarifas.repositories.TarifaTerrestreOrigenLCLRepository;
import co.com.transborder.mstarifas.repositories.TarifasVigenteRepository;
import co.com.transborder.mstarifas.services.IntervencionPricing;
import co.com.transborder.mstarifas.services.IntervencionPricingLclService;

@Service
//@Slf4j
public class IntervencionPricingLclServiceImpl extends IntervencionPricing implements IntervencionPricingLclService {

    @Autowired
    EjecucionDiariaTarifasVigentesRepository ejecucionDiariaRepository;
    
    @Autowired
    CotizacionSpRepository cotizacionSpRepository ;

    @Autowired
    IntervencionPricingLclRepository repoIntervencionPricing;
    
    @Autowired
    IntervencionTarifaFleteInternacionalLclRepository repoIntervTarifaNavieraLcl;   
    
    
    @Autowired
    IntervencionRecargosCarrierDestinoLclRepository repoIntervencionRecargosDestinoLcl;
    
    
    @Autowired
    IntervencionRecargosCarrierOrigenLclRepository repoIntervRecargosOrigenLcl;
    
    @Autowired
    IntervencionGastosOrigenLclRepository repoIntervGastosOrigenLcl;
    
    @Autowired
    IntervencionGastosDestinoLclRepository repoIntervGastosDestinoLcl;
    
    @Autowired
    IntervencionTerrestreDestinoLclRepository repoIntervTerrestreDestinoLcl;    
    
    @Autowired
    IntervencionTerrestreOrigenLclRepository repoIntervTerrestreOrigenLcl;
    
    @Autowired
    IntervencionDevolucionContenedorDestinoRepository repoIntervDevolucionContenedorVacio; 
    
    
    @Autowired  
    IntervencionPricingDynamicRepository repoIntervencionPricingDynamic;
    
    @Autowired
    ConsultaTarifariosDynamicRepository repoConsultaTarifariosDynamic;
    
    @Autowired
    IAuthenticationFacade authenticationFacade;
    
    @Autowired
    TarifaEspecificaRepository tarifaEspecificaRepository;
    
    IntervencionPricingEntity princingEntity = null;

    
    @Override
    public void ejecutarTarifas(LocalDate date) {               
        cotizacionSpRepository.executeSpActualizarVigenciasFuturas(date);
    }
    
        
    @Override
    public LocalDate consultarFechaDesde() {
        Object[] ejecucion = ejecucionDiariaRepository.getUltimaEjecucion();
        LocalDate fecha = LocalDate.now();
        Object[] data = (Object[])ejecucion[0];
        if (data != null && data.length > 0) {
            Date fechaInicio = data[0] != null ? (Date) data[0]: null;
            Date fechaFin = data[1] != null ? (Date) data[1]: null;
            
            if (fechaInicio == null && fechaFin == null) {
                return fecha; 
            }
            if (fechaFin != null) {
                fecha = fechaFin.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().plusDays(1);
            } else {
                fecha = fechaInicio.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            }
        } 
        return fecha;
    }
    
    @Override
    public IntervencionTarifasDTO consultaTarifasIntervPricing(ConsultaTarifasIntervPricingDTO dto) {

        IntervencionTarifasDTO tarifasDTO = new IntervencionTarifasDTO();
        
        if (dto != null) {
        	if (dto.getTipoTransporte().equals(TipoTransporteEnum.MARITIMO)) {
                
                tarifasDTO.setListFleteInternacionalLcl(getFleteInternacionalLcl(dto));                
                
                tarifasDTO.setListRecargosOrigenLcl(getRecargosCarrierOrigen(dto));                
                
                tarifasDTO.setListRecargosDestinoLcl(getRecargosCarrierDestino(dto));                              
                
                eliminarNavierasContratoNoComunes(tarifasDTO.getListFleteInternacionalLcl(), tarifasDTO.getListRecargosOrigenLcl(), tarifasDTO.getListRecargosDestinoLcl());
                
                tarifasDTO.setListGastosOrigenLcl(getGastosOrigenLcl(dto));
                
                tarifasDTO.setListGastosDestinoLcl(getGastosDestinoLcl(dto));


            } else if (dto.getTipoTransporte().equals(TipoTransporteEnum.TERRESTRE)) {
                
                tarifasDTO.setListTerrestreOrigenLcl(getTarifaTerrOriLcl(dto));
                
                tarifasDTO.setListTerrestreDestinoLcl(getTarifaTerrDestLcl(dto));
                            
            }                
        }        
        
        return tarifasDTO;
    }
    
    
    

    private List<IntervencionTarifaFletesInternacionalLclDTO> getFleteInternacionalLcl(ConsultaTarifasIntervPricingDTO dto) {
    	List<Object[]> data = new ArrayList<>();
    	List<IntervencionTarifaFletesInternacionalLclDTO> list = new ArrayList<>();
    	if (dto.getTipoTarifa() != null) {
    		  data = repoIntervencionPricing.findFleteInternacionalLcl(dto.getTipoTarifa().name(), dto.getIdPuertoOrigen(), dto.getIdPuertoDestino(), dto.getTipoTarifa().equals(TipoTarifaEnum.FAK)?null:dto.getTarifaEspecifica(), 
    	        				dto.getFechaVigenciaTarifa()) ;
    	}
        
         if( !data.isEmpty() ) {
            data.forEach( item ->{
                Boolean activo = item[34]!=null ? (Boolean)item[34]:null;
                if (dto.getPendienteGestion() != null && dto.getPendienteGestion() &&
                        activo != null && activo) {
                    return;
                }
                IntervencionTarifaFletesInternacionalLclDTO fleteInt = new IntervencionTarifaFletesInternacionalLclDTO();
                NavieraDTO naviera = new NavieraDTO();
                                
                fleteInt.setIdTarifaFleteInternacionalLcl(Long.valueOf(item[0].toString()));
                //fleteInt.setId (item[1]!=null?Long.valueOf(item[1].toString()):null);
                fleteInt.setVigenciaDesde((Date)item[2]);
                fleteInt.setVigenciaHasta((Date)item[3]);               
                
                fleteInt.setMoneda(item[4] != null? new MonedaDTO((String)item[4]) : null);   
                
                naviera.setId(Long.valueOf(String.valueOf(item[5])));
                naviera.setCodigo(item[6].toString());
                naviera.setNombre(item[7].toString());
                fleteInt.setNaviera(naviera);
                                
                //fleteInt.setTipoTarifa(TipoTarifaEnum.valueOf(item[6].toString()));
                                
                fleteInt.setFleteTonM3Venta(item[8] != null  ? Double.valueOf(item[8].toString()) : 0);
                fleteInt.setMinimaFleteVenta(item[9] != null  ? Double.valueOf(item[9].toString()) : 0);
                fleteInt.setRecargosFleteTonM3Venta(item[10] != null  ? Double.valueOf(item[10].toString()) : 0);
                fleteInt.setMinimaRecargosFleteVenta(item[11] != null  ? Double.valueOf(item[11].toString()) : 0);
                fleteInt.setOtrosTonM3Venta(item[12] != null  ? Double.valueOf(item[12].toString()) : 0);
                fleteInt.setMinimaOtrosVenta(item[13] != null  ? Double.valueOf(item[13].toString()) : 0);
                fleteInt.setMercanciaPeligrosaVenta(item[14] != null  ? Double.valueOf(item[14].toString()) : 0);
                fleteInt.setOverweightVenta(item[15] != null  ? Double.valueOf(item[15].toString()) : 0);
                
                if(item[16]!=null) {                    
                    FormaIntervencionEnum formaIntervencion = FormaIntervencionEnum.valueOf(item[16].toString());   
                    
                    fleteInt.setFleteTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, fleteInt.getFleteTonM3Venta(), item[17], item[18]));
                    fleteInt.setMinimaFleteVentaAnt(calcularValorIntervencion(formaIntervencion, fleteInt.getMinimaFleteVenta(), item[19], item[20]));
                    fleteInt.setRecargosFleteTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, fleteInt.getRecargosFleteTonM3Venta(), item[21], item[22]));
                    fleteInt.setMinimaRecargosFleteVentaAnt(calcularValorIntervencion(formaIntervencion, fleteInt.getMinimaRecargosFleteVenta(), item[23], item[24]));
                    fleteInt.setOtrosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, fleteInt.getOtrosTonM3Venta(), item[25], item[26]));
                    fleteInt.setMinimaOtrosVentaAnt(calcularValorIntervencion(formaIntervencion, fleteInt.getMinimaOtrosVenta(), item[27], item[28]));
                    fleteInt.setMercanciaPeligrosaVentaAnt(calcularValorIntervencion(formaIntervencion, fleteInt.getMercanciaPeligrosaVenta(), item[29], item[30]));
                    fleteInt.setOverweightVentaAnt(calcularValorIntervencion(formaIntervencion, fleteInt.getOverweightVenta(), item[31], item[32]));
                    
                }
                
                fleteInt.setNumeroContratoFlete(item[33] != null ? String.valueOf(item[33].toString()) : null);
                
                
                list.add(fleteInt);
            });         
         }
         return (list);
    }
    
    
    private List<IntervencionRecargosCarrierOrigenLclDTO> getRecargosCarrierOrigen(ConsultaTarifasIntervPricingDTO dto) {
        
        List<Object[]> data = 
        		repoIntervencionPricing.findRecargosCarrierOrigenLcl(dto.getIdPuertoOrigen(), dto.getIdPuertoDestino(), dto.getFechaVigenciaTarifa()) ;
        			
        List<IntervencionRecargosCarrierOrigenLclDTO> list = new ArrayList<>();
        
          if( !data.isEmpty() ) {
            data.forEach( item ->{
                Boolean activo = item[40]!=null ? (Boolean)item[40]:null;
                if (dto.getPendienteGestion() != null && dto.getPendienteGestion() &&
                        activo != null && activo) {
                    return;
                }
                IntervencionRecargosCarrierOrigenLclDTO carrierOrigenDto = new IntervencionRecargosCarrierOrigenLclDTO();
                NavieraDTO naviera = new NavieraDTO();

                carrierOrigenDto.setIdTarifaRecargosCarrierOrigenLcl(Long.valueOf(item[0].toString()));
                //carrierOrigenDto.setId (item[1]!=null?Long.valueOf(item[1].toString()):null);
                carrierOrigenDto.setVigenciaDesde((Date)item[2]);
                carrierOrigenDto.setVigenciaHasta((Date)item[3]);               
                
                carrierOrigenDto.setMoneda(item[4] != null? new MonedaDTO((String)item[4]) : null);   
                
                naviera.setId(Long.valueOf(String.valueOf(item[5])));
                naviera.setCodigo(item[6].toString());
                naviera.setNombre(item[7].toString());
                carrierOrigenDto.setNaviera(naviera);
                
                //recargos m3
                carrierOrigenDto.setConsolidacionTonM3Venta(item[8] != null ? Double.valueOf(item[8].toString()) : 0);
                carrierOrigenDto.setMinimaConsolidacionVenta(item[9] != null ? Double.valueOf(item[9].toString()) : 0);
                carrierOrigenDto.setRecargosTonM3Venta(item[10] != null ? Double.valueOf(item[10].toString()) : 0);
                carrierOrigenDto.setMinimaRecargosVenta(item[11] != null ? Double.valueOf(item[11].toString()) : 0);
                carrierOrigenDto.setOtrosTonM3Venta(item[12] != null ? Double.valueOf(item[12].toString()) : 0);
                carrierOrigenDto.setMinimaOtrosVenta(item[13] != null ? Double.valueOf(item[13].toString()) : 0);               

                //BL
                carrierOrigenDto.setDocFeeVenta(item[14] != null ? Double.valueOf(item[14].toString()) : 0);
                carrierOrigenDto.setEmisionBlVenta(item[15] != null ? Double.valueOf(item[15].toString()) : 0);
                carrierOrigenDto.setRecargosBlVenta(item[16] != null ? Double.valueOf(item[16].toString()) : 0);
                carrierOrigenDto.setOtrosBlVenta(item[17] != null ? Double.valueOf(item[17].toString()) : 0);
                                
                
                if(item[18]!=null) {                    
                    FormaIntervencionEnum formaIntervencion = FormaIntervencionEnum.valueOf(item[18].toString());
                                                            
                    carrierOrigenDto.setConsolidacionTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getConsolidacionTonM3Venta(), item[19], item[20]));
                    carrierOrigenDto.setMinimaConsolidacionVentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getMinimaConsolidacionVenta(), item[21], item[22]));
                    carrierOrigenDto.setRecargosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getRecargosTonM3Venta(), item[23], item[24]));
                    carrierOrigenDto.setMinimaRecargosVentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getMinimaRecargosVenta(), item[25], item[26]));
                    carrierOrigenDto.setOtrosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getOtrosTonM3Venta(), item[27], item[28]));
                    carrierOrigenDto.setMinimaOtrosVentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getMinimaOtrosVenta(), item[29], item[30]));
                    //bl
                    carrierOrigenDto.setDocFeeVentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getDocFeeVenta(), item[31], item[32]));
                    carrierOrigenDto.setEmisionBlVentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getEmisionBlVenta(), item[33], item[34]));
                    carrierOrigenDto.setRecargosBlVentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getRecargosBlVenta(), item[35], item[36]));
                    carrierOrigenDto.setOtrosBlVentaAnt(calcularValorIntervencion(formaIntervencion, carrierOrigenDto.getOtrosBlVenta(), item[37], item[38]));
                                
                }               
                carrierOrigenDto.setNumeroContratoFlete(item[39] != null ? String.valueOf(item[39].toString()) : null);
                
                list.add(carrierOrigenDto);
            });
            
        }
          
          return list;
    }


    private List<IntervencionRecargosCarrierDestinoLclDTO> getRecargosCarrierDestino(ConsultaTarifasIntervPricingDTO dto) {
        
        List<Object[]> data = 
        		repoIntervencionPricing.findRecargosCarrierDestinoLcl(dto.getIdPuertoOrigen(), dto.getIdPuertoDestino(), dto.getFechaVigenciaTarifa()) ;
        			
        List<IntervencionRecargosCarrierDestinoLclDTO> list = new ArrayList<>();
       
          if( !data.isEmpty() ) {
            data.forEach( item ->{
                Boolean activo = item[40]!=null ? (Boolean)item[40]:null;
                if (dto.getPendienteGestion() != null && dto.getPendienteGestion() &&
                        activo != null && activo) {
                    return;
                }
                IntervencionRecargosCarrierDestinoLclDTO carrierDestinoDto = new IntervencionRecargosCarrierDestinoLclDTO();
                NavieraDTO naviera = new NavieraDTO();

                carrierDestinoDto.setIdTarifaRecargosCarrierDestinoLcl(Long.valueOf(item[0].toString()));
                //carrierDestinoDto.setId (item[1]!=null?Long.valueOf(item[1].toString()):null);
                carrierDestinoDto.setVigenciaDesde((Date)item[2]);
                carrierDestinoDto.setVigenciaHasta((Date)item[3]);              
                
                carrierDestinoDto.setMoneda(item[4] != null? new MonedaDTO((String)item[4]) : null);   
                
                naviera.setId(Long.valueOf(String.valueOf(item[5])));
                naviera.setCodigo(item[6].toString());
                naviera.setNombre(item[7].toString());
                carrierDestinoDto.setNaviera(naviera);
                
                //tom3
                carrierDestinoDto.setDesconsolidacionTonM3Venta(item[8] != null ? Double.valueOf(item[8].toString()) : 0);
                carrierDestinoDto.setMinimaDesconsolidacionVenta(item[9] != null ? Double.valueOf(item[9].toString()) : 0);
                carrierDestinoDto.setRecargosTonM3Venta(item[10] != null ? Double.valueOf(item[10].toString()) : 0);
                carrierDestinoDto.setMinimaRecargosVenta(item[11] != null ? Double.valueOf(item[11].toString()) : 0);
                carrierDestinoDto.setOtrosTonM3Venta(item[12] != null ? Double.valueOf(item[12].toString()) : 0);
                carrierDestinoDto.setMinimaOtrosVenta(item[13] != null ? Double.valueOf(item[13].toString()) : 0);
                //BL
                carrierDestinoDto.setDocFeeVenta(item[14] != null ? Double.valueOf(item[14].toString()) : 0);
                carrierDestinoDto.setEmisionBlVenta(item[15] != null ? Double.valueOf(item[15].toString()) : 0);
                carrierDestinoDto.setRecargosBlVenta(item[16] != null ? Double.valueOf(item[16].toString()) : 0);
                carrierDestinoDto.setOtrosBlVenta(item[17] != null ? Double.valueOf(item[17].toString()) : 0);
                
                if(item[18]!=null) {                    
                    FormaIntervencionEnum formaIntervencion = FormaIntervencionEnum.valueOf(item[18].toString());
                       //TOM3                                     
                    carrierDestinoDto.setDesconsolidacionTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getDesconsolidacionTonM3Venta(), item[19], item[20]));
                    carrierDestinoDto.setMinimaDesconsolidacionVentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getMinimaDesconsolidacionVenta(), item[21], item[22]));
                    carrierDestinoDto.setRecargosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getRecargosTonM3Venta(), item[23], item[24]));
                    carrierDestinoDto.setMinimaRecargosVentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getMinimaRecargosVenta(), item[25], item[26]));
                    carrierDestinoDto.setOtrosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getOtrosTonM3Venta(), item[27], item[28]));
                    carrierDestinoDto.setMinimaOtrosVentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getMinimaOtrosVenta(), item[29], item[30]));

                    //BL
                    carrierDestinoDto.setDocFeeVentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getDocFeeVenta(), item[31], item[32]));
                    carrierDestinoDto.setEmisionBlVentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getEmisionBlVenta(), item[33], item[34]));
                    carrierDestinoDto.setRecargosBlVentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getRecargosBlVenta(), item[35], item[36]));
                    carrierDestinoDto.setOtrosBlVentaAnt(calcularValorIntervencion(formaIntervencion, carrierDestinoDto.getOtrosBlVenta(), item[37], item[38]));                    
                }           
                    
                carrierDestinoDto.setNumeroContratoFlete(item[39] != null ? String.valueOf(item[39].toString()) : null);
                
                list.add(carrierDestinoDto);
            });
            
        }
          
          return list;
    }
    

    

    private List<IntervencionGastosOrigenLclDTO> getGastosOrigenLcl(ConsultaTarifasIntervPricingDTO dto) {
        
        List<Object[]> data = 
        		repoIntervencionPricing.findTarifaGastosOrigenLcl(dto.getIdPuertoOrigen(), dto.getIdPuertoDestino(), dto.getFechaVigenciaTarifa()) ; 
        			
        List<IntervencionGastosOrigenLclDTO> list = new ArrayList<IntervencionGastosOrigenLclDTO>();
         if( !data.isEmpty() ) {
            data.forEach( item ->{
                Boolean activo = item[30]!=null ? (Boolean)item[30]:null;
                if (dto.getPendienteGestion() != null && dto.getPendienteGestion() &&
                        activo != null && activo) {
                    return;
                }
                IntervencionGastosOrigenLclDTO gastosOri = new IntervencionGastosOrigenLclDTO();
                
                gastosOri.setIdTarifaGastosOrigenLcl(Long.valueOf(item[0].toString()));
                
                //gastosOri.setId(item[1]!=null?Long.valueOf(item[1].toString()):null);
                gastosOri.setVigenciaDesde((Date)item[2]);
                gastosOri.setVigenciaHasta((Date)item[3]);              
                
                gastosOri.setMoneda(item[4] != null? new MonedaDTO((String)item[4]) : null);                                                
                
                gastosOri.setAduanaVenta(item[5] != null ? Double.valueOf(item[5].toString()) : 0);
                gastosOri.setCostosPortuariosVenta(item[6] != null ? Double.valueOf(item[6].toString()) : 0);
                gastosOri.setGastosOperacionVenta(item[7] != null ? Double.valueOf(item[7].toString()) : 0);
                gastosOri.setOtrosGastosOperacionVenta(item[8] != null ? Double.valueOf(item[8].toString()) : 0);

                gastosOri.setHandlingVenta(item[9] != null ? Double.valueOf(item[9].toString()) : 0);
                gastosOri.setDocumentacionVenta(item[10] != null ? Double.valueOf(item[10].toString()) : 0);
                gastosOri.setGastosBlVenta(item[11] != null ? Double.valueOf(item[11].toString()) : 0);
                gastosOri.setOtrosGastosBlVenta(item[12] != null ? Double.valueOf(item[12].toString()) : 0);
                
                if(item[13]!=null) {                    
                    FormaIntervencionEnum formaIntervencion = FormaIntervencionEnum.valueOf(item[13].toString());   
                    
                    gastosOri.setAduanaVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getAduanaVenta(), item[14], item[15]));
                    gastosOri.setCostosPortuariosVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getCostosPortuariosVenta(), item[16], item[17]));
                    gastosOri.setGastosOperacionVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getGastosOperacionVenta(), item[18], item[19]));
                    gastosOri.setOtrosGastosOperacionVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getOtrosGastosOperacionVenta(), item[20], item[21]));
                    gastosOri.setHandlingVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getHandlingVenta(), item[22], item[23]));
                    gastosOri.setDocumentacionVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getDocumentacionVenta(), item[24], item[25]));
                    gastosOri.setGastosBlVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getGastosBlVenta(), item[26], item[27]));
                    gastosOri.setOtrosGastosBlVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getOtrosGastosBlVenta(), item[28], item[29]));
                }
                
                list.add(gastosOri);
                
            });
         }
         return list;
        
    }
        

    private List<IntervencionGastosDestinoLclDTO> getGastosDestinoLcl(ConsultaTarifasIntervPricingDTO dto) {
        
        List<Object[]> data =  
        		repoIntervencionPricing.findTarifaGastosDestinoLcl(dto.getIdPuertoOrigen(), dto.getIdPuertoDestino(), dto.getFechaVigenciaTarifa()) ; 
        			
        List<IntervencionGastosDestinoLclDTO> list = new ArrayList<IntervencionGastosDestinoLclDTO>();
         if( !data.isEmpty() ) {
            data.forEach( item ->{
                Boolean activo = item[30]!=null ? (Boolean)item[30]:null;
                if (dto.getPendienteGestion() != null && dto.getPendienteGestion() &&
                        activo != null && activo) {
                    return;
                }
                IntervencionGastosDestinoLclDTO gastosOri = new IntervencionGastosDestinoLclDTO();
                
                gastosOri.setIdTarifaGastosDestinoLcl(Long.valueOf(item[0].toString()));
                
                //gastosOri.setId(item[1]!=null?Long.valueOf(item[1].toString()):null);
                gastosOri.setVigenciaDesde((Date)item[2]);
                gastosOri.setVigenciaHasta((Date)item[3]);              
                
                gastosOri.setMoneda(item[4] != null? new MonedaDTO((String)item[4]) : null);                                                
                
                gastosOri.setAduanaVenta(item[5] != null ? Double.valueOf(item[5].toString()) : 0);
                gastosOri.setCostosPortuariosVenta(item[6] != null ? Double.valueOf(item[6].toString()) : 0);
                gastosOri.setGastosOperacionVenta(item[7] != null ? Double.valueOf(item[7].toString()) : 0);
                gastosOri.setOtrosGastosOperacionVenta(item[8] != null ? Double.valueOf(item[8].toString()) : 0);
                gastosOri.setHandlingVenta(item[9] != null ? Double.valueOf(item[9].toString()) : 0);
                gastosOri.setDocumentacionVenta(item[10] != null ? Double.valueOf(item[10].toString()) : 0);
                gastosOri.setGastosBlVenta(item[11] != null ? Double.valueOf(item[11].toString()) : 0);
                gastosOri.setOtrosGastosBlVenta(item[12] != null ? Double.valueOf(item[12].toString()) : 0);
                
                if(item[13]!=null) {                    
                    FormaIntervencionEnum formaIntervencion = FormaIntervencionEnum.valueOf(item[13].toString());   
                    
                    gastosOri.setAduanaVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getAduanaVenta(), item[14], item[15]));
                    gastosOri.setCostosPortuariosVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getCostosPortuariosVenta(), item[16], item[17]));
                    gastosOri.setGastosOperacionVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getGastosOperacionVenta(), item[18], item[19]));
                    gastosOri.setOtrosGastosOperacionVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getOtrosGastosOperacionVenta(), item[20], item[21]));
                    gastosOri.setHandlingVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getHandlingVenta(), item[22], item[23]));
                    gastosOri.setDocumentacionVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getDocumentacionVenta(), item[24], item[25]));
                    gastosOri.setGastosBlVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getGastosBlVenta(), item[26], item[27]));
                    gastosOri.setOtrosGastosBlVentaAnt(calcularValorIntervencion(formaIntervencion, gastosOri.getOtrosGastosBlVenta(), item[28], item[29]));
                }
                
                list.add(gastosOri);
                
            });
         }
         return list;
        
    }


    private List<IntervencionTerrestreOrigenLclDTO> getTarifaTerrOriLcl(ConsultaTarifasIntervPricingDTO dto) {
        List<IntervencionTerrestreOrigenLclDTO> listTarifaTerrOriLcl = new ArrayList<>();
        //Origen  = puertoDestino
        List<Object[]> data =   
        		repoIntervencionPricing.findTarifaTerrOriLcl(dto.getIdPuertoOrigen(), dto.getIdCiudadRecogida(), dto.getFechaVigenciaTarifa());
        			
        if( !data.isEmpty() ) {
            data.forEach( item ->{
                Boolean activo = item[24]!=null ? (Boolean)item[24]:null;
                if (dto.getPendienteGestion() != null && dto.getPendienteGestion() &&
                        activo != null && activo) {
                    return;
                }
                IntervencionTerrestreOrigenLclDTO tarifaTerrOri = new IntervencionTerrestreOrigenLclDTO();
                
                tarifaTerrOri.setIdTarifaTerrestreOrigenLcl(Long.valueOf(item[0].toString()));
                
                //tarifaTerrOri.setId(item[1]!=null?Long.valueOf(item[1].toString()):null);
                tarifaTerrOri.setVigenciaDesde((Date)item[2]);
                tarifaTerrOri.setVigenciaHasta((Date)item[3]);              
                
                tarifaTerrOri.setMoneda(item[4] != null? new MonedaDTO((String)item[4]) : null);                   
              
                tarifaTerrOri.setTonM3Venta(item[5] != null ? Double.valueOf(item[5].toString()) : 0);
                tarifaTerrOri.setMinimaVenta(item[6] != null ? Double.valueOf(item[6].toString()) : 0);
                tarifaTerrOri.setGastosTonM3Venta(item[7] != null ? Double.valueOf(item[7].toString()) : 0);
                tarifaTerrOri.setMinimaGastosVenta(item[8] != null ? Double.valueOf(item[8].toString()) : 0);
                tarifaTerrOri.setOtrosTonM3Venta(item[9] != null ? Double.valueOf(item[9].toString()) : 0);
                tarifaTerrOri.setMinimaOtrosVenta(item[10] != null ? Double.valueOf(item[10].toString()) : 0);
                
                
                if(item[11]!=null) {                    
                    FormaIntervencionEnum formaIntervencion = FormaIntervencionEnum.valueOf(item[11].toString());
                    
                    tarifaTerrOri.setTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrOri.getTonM3Venta(), item[12], item[13]));
                    tarifaTerrOri.setMinimaVentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrOri.getMinimaVenta(), item[14], item[15]));
                    tarifaTerrOri.setGastosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrOri.getGastosTonM3Venta(), item[16], item[17]));
                    tarifaTerrOri.setMinimaGastosVentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrOri.getMinimaGastosVenta(), item[18], item[19]));
                    tarifaTerrOri.setOtrosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrOri.getOtrosTonM3Venta(), item[20], item[21]));
                    tarifaTerrOri.setMinimaOtrosVentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrOri.getMinimaOtrosVenta(), item[22], item[23]));
                    
                }
                
                listTarifaTerrOriLcl.add(tarifaTerrOri);

            });
        }
        return listTarifaTerrOriLcl;
    }
    
    
    private List<IntervencionTerrestreDestinoLclDTO> getTarifaTerrDestLcl(ConsultaTarifasIntervPricingDTO dto) {
        List<IntervencionTerrestreDestinoLclDTO> listTarifaTerrDestLcl = new ArrayList<>();
        // Destino = puertoOrigen
        List<Object[]> data =  
        		repoIntervencionPricing.findTarifaTerrDestLcl(dto.getIdPuertoDestino(), dto.getIdCiudadEntrega(), dto.getFechaVigenciaTarifa());
        			
        if( !data.isEmpty() ) {
            data.forEach( item ->{
                Boolean activo = item[24]!=null ? (Boolean)item[24]:null;
                if (dto.getPendienteGestion() != null && dto.getPendienteGestion() &&
                        activo != null && activo) {
                    return;
                }
                IntervencionTerrestreDestinoLclDTO tarifaTerrDest = new IntervencionTerrestreDestinoLclDTO();
                
                tarifaTerrDest.setIdTarifaTerrestreDestinoLcl(Long.valueOf(item[0].toString()));
                
                tarifaTerrDest.setId(item[1]!=null?Long.valueOf(item[1].toString()):null);
                tarifaTerrDest.setVigenciaDesde((Date)item[2]);
                tarifaTerrDest.setVigenciaHasta((Date)item[3]);             
                
                tarifaTerrDest.setMoneda(item[4] != null? new MonedaDTO((String)item[4]) : null);
                
                tarifaTerrDest.setTonM3Venta(item[5] != null ? Double.valueOf(item[5].toString()) : 0);
                tarifaTerrDest.setMinimaVenta(item[6] != null ? Double.valueOf(item[6].toString()) : 0);
                tarifaTerrDest.setGastosTonM3Venta(item[7] != null ? Double.valueOf(item[7].toString()) : 0);
                tarifaTerrDest.setMinimaGastosVenta(item[8] != null ? Double.valueOf(item[8].toString()) : 0);
                tarifaTerrDest.setOtrosTonM3Venta(item[9] != null ? Double.valueOf(item[9].toString()) : 0);
                tarifaTerrDest.setMinimaOtrosVenta(item[10] != null ? Double.valueOf(item[10].toString()) : 0);
                
                
                if(item[11]!=null) {                    
                    FormaIntervencionEnum formaIntervencion = FormaIntervencionEnum.valueOf(item[11].toString());
                    
                    tarifaTerrDest.setTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrDest.getTonM3Venta(), item[12], item[13]));
                    tarifaTerrDest.setMinimaVentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrDest.getMinimaVenta(), item[14], item[15]));
                    tarifaTerrDest.setGastosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrDest.getGastosTonM3Venta(), item[16], item[17]));
                    tarifaTerrDest.setMinimaGastosVentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrDest.getMinimaGastosVenta(), item[18], item[19]));
                    tarifaTerrDest.setOtrosTonM3VentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrDest.getOtrosTonM3Venta(), item[20], item[21]));
                    tarifaTerrDest.setMinimaOtrosVentaAnt(calcularValorIntervencion(formaIntervencion, tarifaTerrDest.getMinimaOtrosVenta(), item[22], item[23]));
                    
                }
                
                listTarifaTerrDestLcl.add(tarifaTerrDest);

            });
        }
        return listTarifaTerrDestLcl;
    }
    
    
   
    
    private void eliminarNavierasContratoNoComunes(List<IntervencionTarifaFletesInternacionalLclDTO> fletesInterLcl, 
    	      List<IntervencionRecargosCarrierOrigenLclDTO> recargosCarrierOrigenLcl, List<IntervencionRecargosCarrierDestinoLclDTO> recargosCarrierDestinoLcl) {
    	    
    	 	Boolean hayRecargosOrigen = recargosCarrierOrigenLcl != null && !recargosCarrierOrigenLcl.isEmpty();
    	 	Boolean hayRecargosDestino = recargosCarrierDestinoLcl != null && !recargosCarrierDestinoLcl.isEmpty();
    	 	Boolean hayFletes = fletesInterLcl != null && !fletesInterLcl.isEmpty();
    	    
    	     Iterator<IntervencionTarifaFletesInternacionalLclDTO> iterFlete = fletesInterLcl.iterator();
    	     while(iterFlete.hasNext()) {
    	    	 IntervencionTarifaFletesInternacionalLclDTO concepto = iterFlete.next();
    	       if ((hayRecargosOrigen && !recargosCarrierOrigenLcl.stream().filter(conceptos -> conceptos.getNaviera() != null)
    	           .anyMatch(conceptos -> conceptos.getNaviera().getId().equals(concepto.getNaviera().getId()) && conceptos.getNumeroContratoFlete().equals(concepto.getNumeroContratoFlete()))
    	           )&& (hayRecargosDestino && !recargosCarrierDestinoLcl.stream().filter(conceptos -> conceptos.getNaviera() != null)
    	           .anyMatch(conceptos -> conceptos.getNaviera().getId().equals(concepto.getNaviera().getId()) && conceptos.getNumeroContratoFlete().equals(concepto.getNumeroContratoFlete())))) {
    	         iterFlete.remove();
    	       }
    	     }
    	     
    	     Iterator<IntervencionRecargosCarrierOrigenLclDTO> iterRecargoOri = recargosCarrierOrigenLcl.iterator();
    	     while(iterRecargoOri.hasNext()) {
    	       IntervencionRecargosCarrierOrigenLclDTO concepto = iterRecargoOri.next();
    	       if ((hayRecargosDestino && !recargosCarrierDestinoLcl.stream().filter(conceptos -> conceptos.getNaviera() != null)
        	           .anyMatch(conceptos -> conceptos.getNaviera().getId().equals(concepto.getNaviera().getId()) && conceptos.getNumeroContratoFlete().equals(concepto.getNumeroContratoFlete())))
        	           && (hayFletes && !fletesInterLcl.stream().filter(conceptos -> conceptos.getNaviera() != null)
    	           .anyMatch(conceptos -> conceptos.getNaviera().getId().equals(concepto.getNaviera().getId()) && conceptos.getNumeroContratoFlete().equals(concepto.getNumeroContratoFlete())))) {
    	         iterRecargoOri.remove();
    	       }
    	     }
    	     
    	     Iterator<IntervencionRecargosCarrierDestinoLclDTO> iterRecargoDest = recargosCarrierDestinoLcl.iterator();
    	       while(iterRecargoDest.hasNext()) {
    	         IntervencionRecargosCarrierDestinoLclDTO concepto = iterRecargoDest.next();
    	         if ((hayRecargosOrigen && !recargosCarrierOrigenLcl.stream().filter(conceptos -> conceptos.getNaviera() != null)
          	           .anyMatch(conceptos -> conceptos.getNaviera().getId().equals(concepto.getNaviera().getId()) && conceptos.getNumeroContratoFlete().equals(concepto.getNumeroContratoFlete())))
    	             && (hayFletes && !fletesInterLcl.stream().filter(conceptos -> conceptos.getNaviera() != null)
    	      	           .anyMatch(conceptos -> conceptos.getNaviera().getId().equals(concepto.getNaviera().getId()) && conceptos.getNumeroContratoFlete().equals(concepto.getNumeroContratoFlete())))) {
    	           iterRecargoDest.remove();
    	         }
    	       }
    	    
    	    
    	  }
    
    
    
    // *************************** GUARDAR DATOS *************************************************************************************
    
    @Override
    @Transactional
    public void crearPricingLcl(IntervencionTarifasPricingDTO dto) {
        
        if(dto != null) {
                        
            var userDTO = authenticationFacade.getUserInfo();           
            princingEntity = guardaPrincig(dto, userDTO);           
        
            if(dto.getIntervencionTarifasDTO().getListFleteInternacionalLcl()  != null && !dto.getIntervencionTarifasDTO().getListFleteInternacionalLcl().isEmpty()) {
                dto.getIntervencionTarifasDTO().getListFleteInternacionalLcl().forEach((intervencionTarifa) -> {

                    IntervencionTarifaFletesInternacionalLclEntity entity = IntervencionTarifaFleteInternacionalLclMapper.INSTANCE.getEntityFromDTO(intervencionTarifa);
                    
                    if(entity.getId() != null ) {
                        entity.setUsuarioModificacion(userDTO.getUserEmail());
                        entity.setFechaModificacion(new Date());
                    }else {
                        entity.setUsuarioCreacion(userDTO.getUserEmail());
                        entity.setFechaCreacion(new Date());
                    }                   
                    
                    if(entity.getIntervencionPricing() == null)
                        entity.setIntervencionPricing(princingEntity);          
                    
                    repoIntervTarifaNavieraLcl.save(entity);
                    intervencionTarifa.setId(entity.getId());
                    
                    //repoIntervencionPricing.updateTarifaFleteInternacionalLclVigente(entity.getId(), intervencionTarifa.getIdTarifaFleteInternacionalLcl());
                    //repoIntervencionPricing.updateTarifaFleteInternacionalLcl(entity.getId());
                });
                repoIntervencionPricingDynamic.updateTarifarioMaritimoIntervencion(SqlQueriesConstants.UPDATE_TARIFA_FLETES_INTERNACIONAL_LCL_VIGENTE, dto.getConsultaTarifasIntervPricingDTO());
            }
            
        
            if(dto.getIntervencionTarifasDTO().getListRecargosOrigenLcl()  != null && !dto.getIntervencionTarifasDTO().getListRecargosOrigenLcl().isEmpty()) {
                dto.getIntervencionTarifasDTO().getListRecargosOrigenLcl().forEach((intervencionRecargos) -> {
                    final IntervencionRecargosCarrierOrigenLclEntity entity = IntervencionRecargosCarrierOrigenLclMapper.INSTANCE.getEntityFromDTO(intervencionRecargos);
                    
                    if(entity.getId() != null ) {
                        entity.setUsuarioModificacion(userDTO.getUserEmail());
                        entity.setFechaModificacion(new Date());
                    }else {
                        entity.setUsuarioCreacion(userDTO.getUserEmail());
                        entity.setFechaCreacion(new Date());
                    }                   
                    
                    if(entity.getIntervencionPricing() == null)
                        entity.setIntervencionPricing(princingEntity);
                                        
                    repoIntervRecargosOrigenLcl.save(entity);
                    intervencionRecargos.setId(entity.getId());
                    
                    //repoIntervencionPricing.updateTarifaRecargosCarrierOrigenLclVigente(entity.getId(), intervencionRecargos.getIdTarifaRecargosCarrierOrigenLcl());
                    //repoIntervencionPricing.updateTarifaRecargosCarrierOrigenLcl(entity.getId());
                });
                repoIntervencionPricingDynamic.updateTarifarioMaritimoIntervencion(SqlQueriesConstants.UPDATE_TARIFA_RECARGOS_CARRIER_ORIGEN_LCL_VIGENTE, dto.getConsultaTarifasIntervPricingDTO());
            }

        
            if(dto.getIntervencionTarifasDTO().getListRecargosDestinoLcl() != null && !dto.getIntervencionTarifasDTO().getListRecargosDestinoLcl().isEmpty()) {
                dto.getIntervencionTarifasDTO().getListRecargosDestinoLcl().forEach((intervencionRecargos) -> {
                
                    final IntervencionRecargosCarrierDestinoLclEntity entity = IntervencionRecargosCarrierDestinoLclMapper.INSTANCE.getEntityFromDTO(intervencionRecargos);
                    
                    if(entity.getId() != null ) {
                        entity.setUsuarioModificacion(userDTO.getUserEmail());
                        entity.setFechaModificacion(new Date());
                    }else {
                        entity.setUsuarioCreacion(userDTO.getUserEmail());
                        entity.setFechaCreacion(new Date());
                    }
                    
                    
                    if(entity.getIntervencionPricing() == null)
                        entity.setIntervencionPricing(princingEntity);
                    
                    
                    repoIntervencionRecargosDestinoLcl.save(entity);
                    intervencionRecargos.setId(entity.getId());
                    
                    //repoIntervencionPricing.updateTarifaRecargosCarrierDestinoLclVigente(entity.getId(), intervencionRecargos.getIdTarifaRecargosCarrierDestinoLcl());
                    //repoIntervencionPricing.updateTarifaRecargosCarrierDestinoLcl(entity.getId());                   
                });
                repoIntervencionPricingDynamic.updateTarifarioMaritimoIntervencion(SqlQueriesConstants.UPDATE_TARIFA_RECARGOS_CARRIER_DESTINO_LCL_VIGENTE, dto.getConsultaTarifasIntervPricingDTO());
            }

            if(dto.getIntervencionTarifasDTO().getListGastosOrigenLcl() != null && !dto.getIntervencionTarifasDTO().getListGastosOrigenLcl().isEmpty()) {
                dto.getIntervencionTarifasDTO().getListGastosOrigenLcl().forEach((intervencionGastos) -> {
                    
                    final IntervencionGastosOrigenLCLEntity entity = IntervencionGastosOrigenLclMapper.INSTANCE.getEntityFromDTO(intervencionGastos);
                    
                    if(entity.getId() != null ) {
                        entity.setUsuarioModificacion(userDTO.getUserEmail());
                        entity.setFechaModificacion(new Date());
                    }else {
                        entity.setUsuarioCreacion(userDTO.getUserEmail());
                        entity.setFechaCreacion(new Date());
                    }
                                        
                    if(entity.getIntervencionPricing() == null)
                        entity.setIntervencionPricing(princingEntity);
                                        
                    repoIntervGastosOrigenLcl.save(entity);
                    intervencionGastos.setId(entity.getId());
                    
                    //repoIntervencionPricing.updateTarifaGastosOrigenLclVigente(entity.getId(), intervencionGastos.getIdTarifaGastosOrigenLcl());
                    //repoIntervencionPricing.updateTarifaGastosOrigenLcl(entity.getId());
                });
                repoIntervencionPricingDynamic.updateTarifarioMaritimoIntervencion(SqlQueriesConstants.UPDATE_TARIFA_GASTOS_ORIGEN_LCL_VIGENTE, dto.getConsultaTarifasIntervPricingDTO());
            }
            
            if(dto.getIntervencionTarifasDTO().getListGastosDestinoLcl() != null && !dto.getIntervencionTarifasDTO().getListGastosDestinoLcl().isEmpty()) {
                dto.getIntervencionTarifasDTO().getListGastosDestinoLcl().forEach((intervencionGastos) -> {
                    
                    final IntervencionGastosDestinoLCLEntity entity = IntervencionGastosDestinoLclMapper.INSTANCE.getEntityFromDTO(intervencionGastos);
                    
                    if(entity.getId() != null ) {
                        entity.setUsuarioModificacion(userDTO.getUserEmail());
                        entity.setFechaModificacion(new Date());
                    }else {
                        entity.setUsuarioCreacion(userDTO.getUserEmail());
                        entity.setFechaCreacion(new Date());
                    }
                                        
                    if(entity.getIntervencionPricing() == null)
                        entity.setIntervencionPricing(princingEntity);
                                        
                    repoIntervGastosDestinoLcl.save(entity);
                    intervencionGastos.setId(entity.getId());
                    
                    //repoIntervencionPricing.updateTarifaGastosDestinoLclVigente(entity.getId(), intervencionGastos.getIdTarifaGastosDestinoLcl());
                    //repoIntervencionPricing.updateTarifaGastosDestinoLcl(entity.getId());
                });
                repoIntervencionPricingDynamic.updateTarifarioMaritimoIntervencion(SqlQueriesConstants.UPDATE_TARIFA_GASTOS_DESTINO_LCL_VIGENTE, dto.getConsultaTarifasIntervPricingDTO());
            }

        
            if(dto.getIntervencionTarifasDTO().getListTerrestreOrigenLcl() != null && !dto.getIntervencionTarifasDTO().getListTerrestreOrigenLcl().isEmpty()) {
                dto.getIntervencionTarifasDTO().getListTerrestreOrigenLcl().forEach((intervencionTerrestre) -> {
                    final IntervencionTerrestreOrigenLclEntity entity = IntervencionTerrestreOrigenLclMapper.INSTANCE.getEntityFromDTO(intervencionTerrestre);
                    
                    if(entity.getId() != null ) {
                        entity.setUsuarioModificacion(userDTO.getUserEmail());
                        entity.setFechaModificacion(new Date());
                    }else {
                        entity.setUsuarioCreacion(userDTO.getUserEmail());
                        entity.setFechaCreacion(new Date());
                    }
                    
                    if(entity.getIntervencionPricing() == null)
                        entity.setIntervencionPricing(princingEntity);                  
                    
                    repoIntervTerrestreOrigenLcl.save(entity);
                    intervencionTerrestre.setId(entity.getId());
                    
                    //repoIntervencionPricing.updateTarifaTerrestreOrigenLclVigente(entity.getId(), intervencionTerrestre.getIdTarifaTerrestreOrigenLcl());
                    //repoIntervencionPricing.updateTarifaTerrestreOrigenLcl(entity.getId());
                });
                repoIntervencionPricingDynamic.updateTarifarioTerrestreIntervencionOrigen(SqlQueriesConstants.UPDATE_TARIFA_TERRESTRE_ORIGEN_LCL_VIGENTE, dto.getConsultaTarifasIntervPricingDTO());
            }       
        
            if(dto.getIntervencionTarifasDTO().getListTerrestreDestinoLcl() != null && !dto.getIntervencionTarifasDTO().getListTerrestreDestinoLcl().isEmpty()) {
                dto.getIntervencionTarifasDTO().getListTerrestreDestinoLcl().forEach((intervencionTerrestre) -> {
                    final IntervencionTerrestreDestinoLclEntity entity = IntervencionTerrestreDestinoLclMapper.INSTANCE.getEntityFromDTO(intervencionTerrestre);
                    
                    if(entity.getId() != null ) {
                        entity.setUsuarioModificacion(userDTO.getUserEmail());
                        entity.setFechaModificacion(new Date());
                    }else {
                        entity.setUsuarioCreacion(userDTO.getUserEmail());
                        entity.setFechaCreacion(new Date());
                    }
                    
                    if(entity.getIntervencionPricing() == null)
                        entity.setIntervencionPricing(princingEntity);                  
                    
                    repoIntervTerrestreDestinoLcl.save(entity);
                    intervencionTerrestre.setId(entity.getId());
                    
                    //repoIntervencionPricing.updateTarifaTerrestreDestinoLclVigente(entity.getId(), intervencionTerrestre.getIdTarifaTerrestreDestinoLcl());
                    //repoIntervencionPricing.updateTarifaTerrestreDestinoLcl(entity.getId());
                });
                repoIntervencionPricingDynamic.updateTarifarioTerrestreIntervencionDestino(SqlQueriesConstants.UPDATE_TARIFA_TERRESTRE_DESTINO_LCL_VIGENTE, dto.getConsultaTarifasIntervPricingDTO());
            }
    
           
        }
    }
    

    
    private IntervencionPricingEntity guardaPrincig(IntervencionTarifasPricingDTO dto, UserDTO userDTO){
    
            IntervencionPricingDTO pricingDto = new IntervencionPricingDTO();
            PuertoDTO puertoOrigen = new PuertoDTO();
            PuertoDTO puertoDestino = new PuertoDTO();
            CiudadDTO ciudadOrigen = new CiudadDTO();
            CiudadDTO ciudadDestino = new CiudadDTO();
            CiudadDTO ciudadRecogida = new CiudadDTO();
            CiudadDTO ciudadEntrega = new CiudadDTO();
            
            pricingDto.setTipoTransporte(dto.getConsultaTarifasIntervPricingDTO().getTipoTransporte());
            pricingDto.setTipoEmbarque(dto.getConsultaTarifasIntervPricingDTO().getTipoEmbarque());
            pricingDto.setTipoTarifa(dto.getConsultaTarifasIntervPricingDTO().getTipoTarifa());
            pricingDto.setFormaIntervencion(dto.getFormaIntervencion());
            pricingDto.setVigenciaIntervencionDesde(dto.getVigenciaIntervencionDesde());
            pricingDto.setVigenciaIntervencionHasta(dto.getVigenciaIntervencionHasta());
            pricingDto.setAnulada(dto.getAnulada());
            pricingDto.setDescripcionTarifa(dto.getConsultaTarifasIntervPricingDTO().getTarifaEspecifica());
            
            if(dto.getConsultaTarifasIntervPricingDTO().getIdPuertoOrigen() != null) {
                puertoOrigen.setId(dto.getConsultaTarifasIntervPricingDTO().getIdPuertoOrigen());
                pricingDto.setPuertoOrigen(puertoOrigen);
            }
            if (dto.getConsultaTarifasIntervPricingDTO().getIdPuertoDestino() != null) {
                puertoDestino.setId(dto.getConsultaTarifasIntervPricingDTO().getIdPuertoDestino());
                pricingDto.setPuertoDestino(puertoDestino);
            }
            
            if(dto.getConsultaTarifasIntervPricingDTO().getIdCiudadOrigen() != null && dto.getConsultaTarifasIntervPricingDTO().getIdCiudadDestino() != null) {
                ciudadOrigen.setId(dto.getConsultaTarifasIntervPricingDTO().getIdCiudadOrigen());
                ciudadDestino.setId(dto.getConsultaTarifasIntervPricingDTO().getIdCiudadDestino());
                pricingDto.setCiudadOrigen(ciudadOrigen);
                pricingDto.setCiudadDestino(ciudadDestino);
            }
            
            if(dto.getConsultaTarifasIntervPricingDTO().getIdCiudadRecogida() != null && dto.getConsultaTarifasIntervPricingDTO().getIdCiudadEntrega() != null) {
                ciudadRecogida.setId(dto.getConsultaTarifasIntervPricingDTO().getIdCiudadRecogida());
                ciudadEntrega.setId(dto.getConsultaTarifasIntervPricingDTO().getIdCiudadEntrega());
                pricingDto.setCiudadRecogida(ciudadRecogida);
                pricingDto.setCiudadEntrega(ciudadEntrega);
            }
            
            if(pricingDto.getId() != null ) {
                pricingDto.setUsuarioModificacion(userDTO.getUserEmail());
                pricingDto.setFechaModificacion(new Date());
            }else {
                pricingDto.setUsuarioCreacion(userDTO.getUserEmail());
                pricingDto.setFechaCreacion(new Date());
            }
    
            IntervencionPricingEntity entity = IntervencionPricingMapper.INSTANCE.getEntityFromDTO(pricingDto);         
            entity = repoIntervencionPricing.save(entity);
            
            return entity;
    }   
    
    

    @Override
    public List<IntervencionPricingDTO> consultaGestionVigenciaPricing(ConsultaGestionIntervencionPricingDTO dto) {
        
        List<IntervencionPricingDTO> listIntervencionGestionPricingDTO = new ArrayList<>();
        List<IntervencionPricingEntity> listEntity = new ArrayList<>();  
        
            if (dto.getTipoTransporte().equals(TipoTransporteEnum.MARITIMO)) {
                    listEntity = repoIntervencionPricingDynamic.oDataPricingMaritimoDynamic(dto);
                    for (IntervencionPricingEntity intervencionPricingEspEntity : listEntity) {
                        listIntervencionGestionPricingDTO.add(IntervencionPricingMapper.INSTANCE.getDTOFromEntity(intervencionPricingEspEntity));
                }
            } else  if (dto.getTipoTransporte().equals(TipoTransporteEnum.TERRESTRE)) {
                    listEntity = repoIntervencionPricingDynamic.oDataPricingTerrestreDynamic(dto);
                    for (IntervencionPricingEntity intervencionPricingEspEntity : listEntity) {
                        listIntervencionGestionPricingDTO.add(IntervencionPricingMapper.INSTANCE.getDTOFromEntity(intervencionPricingEspEntity));
                }
            }
                    
        return listIntervencionGestionPricingDTO;
    }
    
    @Override
    public IntervencionPricingDTO actualizarIntervencionPricing(IntervencionPricingDTO dto) {
    
        UserDTO userAuth = authenticationFacade.getUserInfo();

        IntervencionPricingEntity entity = IntervencionPricingMapper.INSTANCE.getEntityFromDTO(dto);
        
        if (entity.getId() != null) {
            entity.setUsuarioModificacion(userAuth.getUserEmail());
            entity.setFechaModificacion(new Date());
        } else {
            entity.setUsuarioCreacion(userAuth.getUserEmail());
            entity.setFechaCreacion(new Date());
        }
        
        entity = repoIntervencionPricing.save(entity);
    
        return IntervencionPricingMapper.INSTANCE.getDTOFromEntity(entity);
        
    }


    @Override
    public List<IntervencionControlVigenciasDTO> consultaControlVigenciasIntervencion() {
        return cotizacionSpRepository.obtenerControlIntervencionesVencidas();
    }
    /*
    @Override
    public List<ConsultaTarifariosPorProductoDTO> consultarTarifariosPorProducto(ConsultaTarifariosPorProductoDTO dto) {
        
        List<ConsultaTarifariosPorProductoDTO> listTarifarios = new ArrayList<>();
        
        if(dto != null ) {
    
            if(dto.getTipoTarifario() == null){
                
                List<ConsultaTarifariosPorProductoDTO> dataTemp1 = new ArrayList<>();
                List<ConsultaTarifariosPorProductoDTO> dataTemp2 = new ArrayList<>();
                List<Object[]> data = repoConsultaTarifariosDynamic.oDataTarifariosDynamic(dto);
                List<Object[]> dataSeguro = repoConsultaTarifariosDynamic.oDataTarifariosSeguroDynamic(dto);
                
                dataTemp1 = listTarifariosBasicos(data);
                dataTemp2 = listTarifariosSeguro(dataSeguro);
                listTarifarios = Stream.concat(dataTemp1.stream(), dataTemp2.stream()).collect(Collectors.toList());
            
            }else {
                
                switch (CategoriaCargueArchivoEnum.valueOf(dto.getTipoTarifario())) {
                    case FLETES_INTERNACIONALES_LCL:
                    case RECARGOS_CARRIER_DESTINO_LCL:
                    case RECARGOS_CARRIER_ORIGEN_LCL:
                    case FLETES_INTERNACIONALES_LCL:
                    case RECARGOS_CARRIER_DESTINO_LCL:
                    case RECARGOS_CARRIER_ORIGEN_LCL:
                    case TRANSPORTE_TERRESTRE_ORIGEN_LCL:
                    case TRANSPORTE_TERRESTRE_DESTINO_LCL:
                    case DEVOLUCION_CONTENEDOR_DESTINO_LCL:
                    case TRANSPORTE_TERRESTRE_ORIGEN_LCL:
                    case TRANSPORTE_TERRESTRE_DESTINO_LCL:
                    case GASTOS_ORIGEN_LCL:
                    case GASTOS_DESTINO_LCL:
                    case GASTOS_ORIGEN_LCL:
                    case GASTOS_DESTINO_LCL:
                        List<Object[]> data = repoConsultaTarifariosDynamic.oDataTarifariosDynamic(dto);
                        listTarifarios = listTarifariosBasicos(data);
                    
                        break;
                    case TARIFA_SEGURO:
                        List<Object[]> dataSeguro = repoConsultaTarifariosDynamic.oDataTarifariosSeguroDynamic(dto);
                        listTarifarios = listTarifariosSeguro(dataSeguro);
                        break;
                        
                    default:
                        List<ConsultaTarifariosPorProductoDTO> dataTemp1 = new ArrayList<>();
                        List<ConsultaTarifariosPorProductoDTO> dataTemp2 = new ArrayList<>();
                        
                        List<Object[]> datas = repoConsultaTarifariosDynamic.oDataTarifariosDynamic(dto);
                        List<Object[]> dataSeguros = repoConsultaTarifariosDynamic.oDataTarifariosSeguroDynamic(dto);
                        
                        dataTemp1 = listTarifariosSeguro(dataSeguros);
                        dataTemp2 = listTarifariosBasicos(datas);
                        
                        listTarifarios = Stream.concat(dataTemp1.stream(), dataTemp2.stream()).collect(Collectors.toList());
                
                        break;
                }
            }
            
        }
        
    return listTarifarios;
        
    }   
    */
    
    public List<ConsultaTarifariosPorProductoDTO> listTarifariosBasicos (List<Object[]> data){
        List<ConsultaTarifariosPorProductoDTO> listTarifarios = new ArrayList<>();
        
        if( !data.isEmpty() ) {
            data.forEach( item ->{
                ConsultaTarifariosPorProductoDTO consultaBasica = new ConsultaTarifariosPorProductoDTO();
                consultaBasica.setId(Long.valueOf(String.valueOf(item[0])));
                consultaBasica.setUsuarioResponsable(item[1].toString());
                consultaBasica.setFechaCargue(convertToDateViaSqlTimestamp((LocalDateTime) item[2]));
                String tipoTarifarioTemp = item[3].toString();
                consultaBasica.setTipoTarifario(CategoriaCargueArchivoEnum.valueOf(tipoTarifarioTemp).getDescription());
                
                if(item[4] != null) {
                    consultaBasica.setNombreArchivo(item[4].toString());                    
                }else {
                    consultaBasica.setNombreArchivo(null);
                }
                
                listTarifarios.add(consultaBasica);
            });
        }   
        return listTarifarios;
    }
    
    
    public List<ConsultaTarifariosPorProductoDTO> listTarifariosSeguro (List<Object[]> dataSeguro){
        List<ConsultaTarifariosPorProductoDTO> listTarifarios = new ArrayList<>();
        
        if( !dataSeguro.isEmpty() ) {
            dataSeguro.forEach( item ->{
                ConsultaTarifariosPorProductoDTO consultaSeguro = new ConsultaTarifariosPorProductoDTO();
                consultaSeguro.setId(Long.valueOf(String.valueOf(item[0])));
                consultaSeguro.setUsuarioResponsable(item[1].toString());
                consultaSeguro.setFechaCargue((Date) item[2]);
                String tipoTarifarioTemp = item[3].toString();
                consultaSeguro.setTipoTarifario(TipoTarifarioEnum.valueOf(tipoTarifarioTemp).getDescription());
                consultaSeguro.setNombreArchivo(null);
                listTarifarios.add(consultaSeguro);
            });
        }
        return listTarifarios;
    }
    
    
    public Date convertToDateViaSqlTimestamp(LocalDateTime dateToConvert) {
        return java.sql.Timestamp.valueOf(dateToConvert);
    }


    @Override
    public void deleteIntervencionComercial(Long idCotizacion) {
        cotizacionSpRepository.execDeleteIntervencionComercial(idCotizacion);
        
    }

    @Override
    public CommodityDTO createTarifaEspecifica(CommodityDTO commodity) {
        CommodityEntity entity = tarifaEspecificaRepository.save(TarifaEspecificaMapper.INSTANCE.getEntityFromDTO(commodity));
        return TarifaEspecificaMapper.INSTANCE.getDTOFromEntity(entity);
    }   
    
    @Override
    public List<CommodityDTO> getTarifasEspecificasEstado(EstadoCommodityEnum estado) {
        List<CommodityEntity> tarifasEspecificas = tarifaEspecificaRepository.findByEstado(estado);
        if (tarifasEspecificas != null && !tarifasEspecificas.isEmpty()) {
            return TarifaEspecificaMapper.INSTANCE.getListDTOFromListEntity(tarifasEspecificas);
        }
        return new ArrayList<>();
    }
    
    @Override
    public List<CommodityDTO> getAllTarifasEspecificas() {
        List<Order> orders = new ArrayList<Order>(); 
        var orderPais = new Order(Sort.Direction.ASC, "nombre");
        orders.add(orderPais);
        
        List<CommodityEntity> tarifasEspecificas = tarifaEspecificaRepository.findAll(Sort.by(orders));
        if (tarifasEspecificas != null && !tarifasEspecificas.isEmpty()) {
            return TarifaEspecificaMapper.INSTANCE.getListDTOFromListEntity(tarifasEspecificas);
        }
        return new ArrayList<>();
    }
    
}
